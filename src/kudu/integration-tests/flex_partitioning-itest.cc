// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// Integration test for flexible partitioning (eg buckets, range partitioning
// of PK subsets, etc).

#include <algorithm>
#include <glog/stl_logging.h>
#include <map>
#include <memory>
#include <vector>

#include "kudu/client/client-test-util.h"
#include "kudu/common/partial_row.h"
#include "kudu/integration-tests/external_mini_cluster.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tools/data_gen_util.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/test_util.h"
#include "kudu/gutil/strings/escaping.h"

namespace kudu {
namespace itest {

using client::KuduClient;
using client::KuduClientBuilder;
using client::KuduColumnSchema;
using client::KuduInsert;
using client::KuduPredicate;
using client::KuduScanner;
using client::KuduSchema;
using client::KuduSchemaBuilder;
using client::KuduSession;
using client::KuduTable;
using client::KuduTableCreator;
using client::KuduValue;
using client::sp::shared_ptr;
using std::unordered_map;
using std::vector;
using strings::Substitute;

static const char* const kTableName = "test-table";

struct HashPartitionOptions {
  HashPartitionOptions(vector<string> columns,
                       int32_t num_buckets)
    : columns(std::move(columns)),
      num_buckets(num_buckets) { }

  vector<string> columns;
  int32_t num_buckets;
};

struct RangePartitionOptions {
  RangePartitionOptions(vector<string> columns,
                        vector<vector<int32_t>> splits,
                        vector<pair<vector<int32_t>, vector<int32_t>>> bounds)
    : columns(std::move(columns)),
      splits(std::move(splits)),
      bounds(std::move(bounds)) { }

  vector<string> columns;
  vector<vector<int32_t>> splits;
  vector<pair<vector<int32_t>, vector<int32_t>>> bounds;
};

int NumPartitions(const vector<HashPartitionOptions>& hash_partitions,
                  const RangePartitionOptions& range_partition) {
  int partitions = std::max(1UL, range_partition.bounds.size()) + range_partition.splits.size();
  for (const auto& hash_partition : hash_partitions) {
    partitions *= hash_partition.num_buckets;
  }
  return partitions;
}

string RowToString(const vector<int32_t> row) {
  string s = "(";
  for (int i = 0; i < row.size(); i++) {
    if (i != 0) s.append(", ");
    s.append(std::to_string(row[i]));
  }
  s.append(")");
  return s;
}

string PartitionOptionsToString(const vector<HashPartitionOptions>& hash_partitions,
                                const RangePartitionOptions& range_partition) {
  string s;
  for (const auto& hash_partition : hash_partitions) {
    s.append("HASH (");
    for (int i = 0; i < hash_partition.columns.size(); i++) {
      if (i != 0) s.append(", ");
      s.append(hash_partition.columns[i]);
    }
    s.append(") INTO ");
    s.append(std::to_string(hash_partition.num_buckets));
    s.append(" BUCKETS, ");
  }

  s.append("RANGE (");
  for (int i = 0; i < range_partition.columns.size(); i++) {
    if (i != 0) s.append(", ");
    s.append(range_partition.columns[i]);
  }
  s.append(")");

  if (!range_partition.splits.empty()) {
    s.append(" SPLIT ROWS ");

    for (int i = 0; i < range_partition.splits.size(); i++) {
      if (i != 0) s.append(", ");
      s.append(RowToString(range_partition.splits[i]));
    }
  }

  if (!range_partition.bounds.empty()) {
    s.append(" BOUNDS (");

    for (int i = 0; i < range_partition.bounds.size(); i++) {
      if (i != 0) s.append(", ");
      s.append("[");
      s.append(RowToString(range_partition.bounds[i].first));
      s.append(", ");
      s.append(RowToString(range_partition.bounds[i].second));
      s.append(")");
    }
    s.append(")");
  }
  return s;
}

class FlexPartitioningITest : public KuduTest {
 public:
  FlexPartitioningITest()
    : random_(GetRandomSeed32()) {
  }
  virtual void SetUp() override {
    KuduTest::SetUp();

    ExternalMiniClusterOptions opts;
    opts.num_tablet_servers = 1;
    // This test produces lots of tablets. With container and log preallocation,
    // we end up using quite a bit of disk space. So, we disable them.
    opts.extra_tserver_flags.push_back("--log_container_preallocate_bytes=0");
    opts.extra_tserver_flags.push_back("--log_preallocate_segments=false");
    cluster_.reset(new ExternalMiniCluster(opts));
    ASSERT_OK(cluster_->Start());

    KuduClientBuilder builder;
    ASSERT_OK(cluster_->CreateClient(builder, &client_));
  }

  virtual void TearDown() override {
    cluster_->Shutdown();
    KuduTest::TearDown();
  }

 protected:

  void TestPartitionOptions(const vector<HashPartitionOptions> hash_options,
                            const RangePartitionOptions range_options) {
    CreateTable(hash_options, range_options);
    InsertAndVerifyScans(range_options);
    DeleteTable();

    // Tablets aren't always immediately dropped, spin until they are gone.
    for (int i = 1; CountTablets() > 0; i++) {
      CHECK_LE(i, 30);
      base::SleepForMilliseconds(10 * i);
    }
  }

  void CreateTable(const vector<HashPartitionOptions> hash_partitions,
                   const RangePartitionOptions range_partition) {
    KuduSchemaBuilder b;
    b.AddColumn("c0")->Type(KuduColumnSchema::INT32)->NotNull();
    b.AddColumn("c1")->Type(KuduColumnSchema::INT32)->NotNull();
    b.AddColumn("c2")->Type(KuduColumnSchema::INT32)->NotNull();
    b.SetPrimaryKey({ "c0", "c1", "c2" });
    KuduSchema schema;
    ASSERT_OK(b.Build(&schema));

    gscoped_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    table_creator->table_name(kTableName)
        .schema(&schema)
        .num_replicas(1);

    for (const auto& hash_partition : hash_partitions) {
      table_creator->add_hash_partitions(hash_partition.columns, hash_partition.num_buckets);
    }

    vector<const KuduPartialRow*> split_rows;
    for (const vector<int32_t> split : range_partition.splits) {
      KuduPartialRow* row = schema.NewRow();
      for (int i = 0; i < split.size(); i++) {
        ASSERT_OK(row->SetInt32(range_partition.columns[i], split[i]));
      }
      split_rows.push_back(row);
    }

    table_creator->set_range_partition_columns(range_partition.columns);
    table_creator->split_rows(split_rows);

    for (const auto& bound : range_partition.bounds) {
      KuduPartialRow* lower = schema.NewRow();
      KuduPartialRow* upper = schema.NewRow();

      for (int i = 0; i < bound.first.size(); i++) {
        ASSERT_OK(lower->SetInt32(range_partition.columns[i], bound.first[i]));
      }
      for (int i = 0; i < bound.second.size(); i++) {
        ASSERT_OK(upper->SetInt32(range_partition.columns[i], bound.second[i]));
      }
      table_creator->add_range_bound(lower, upper);
    }

    ASSERT_OK(table_creator->Create());
    ASSERT_OK(client_->OpenTable(kTableName, &table_));
    ASSERT_EQ(NumPartitions(hash_partitions, range_partition), CountTablets());
  }

  void DeleteTable() {
    STLDeleteElements(&inserted_rows_);
    client_->DeleteTable(table_->name());
    table_.reset();
  }

  int CountTablets() {
    unordered_map<string, TServerDetails*> ts_map;
    STLDeleteValues(&ts_map);
    CHECK_OK(itest::CreateTabletServerMap(cluster_->master_proxy().get(),
                                          cluster_->messenger(),
                                          &ts_map));

    vector<tserver::ListTabletsResponsePB_StatusAndSchemaPB> tablets;
    CHECK_OK(ListTablets(ts_map.begin()->second, MonoDelta::FromSeconds(10), &tablets));
    return tablets.size();
  }

  // Insert rows into the given table. The first column 'c0' is ascending,
  // but the rest are random int32s. A single row will be inserted for each
  // unique c0 value in the range bounds. If there are no range bounds, then
  // c0 values [0, 1000) will be used.
  Status InsertRows(const RangePartitionOptions& range_partition, int* row_count);

  // Perform a scan with a predicate on 'col_name' BETWEEN 'lower' AND 'upper'.
  // Verifies that the results match up with applying the same scan against our
  // in-memory copy 'inserted_rows_'.
  void CheckScanWithColumnPredicate(Slice col_name, int lower, int upper);

  // Like the above, but uses the primary key range scan API in the client to
  // scan between 'inserted_rows_[lower]' (inclusive) and 'inserted_rows_[upper]'
  // (exclusive).
  void CheckPKRangeScan(int lower, int upper);
  void CheckPartitionKeyRangeScanWithPKRange(int lower, int upper);

  // Performs a series of scans, each over a single tablet in the table, and
  // verifies that the aggregated results match up with 'inserted_rows_'.
  void CheckPartitionKeyRangeScan();

  // Inserts data into the table, then performs a number of scans to verify that
  // the data can be retrieved.
  void InsertAndVerifyScans(const RangePartitionOptions& range_partition);

  Random random_;

  gscoped_ptr<ExternalMiniCluster> cluster_;

  shared_ptr<KuduClient> client_;
  shared_ptr<KuduTable> table_;
  vector<KuduPartialRow*> inserted_rows_;
};

Status FlexPartitioningITest::InsertRows(const RangePartitionOptions& range_partition, int* row_count) {
  CHECK(inserted_rows_.empty());

  vector<pair<vector<int32_t>, vector<int32_t>>> default_bounds = { { { 0 }, { 1000 } } };
  const vector<pair<vector<int32_t>, vector<int32_t>>>& bounds =
    range_partition.bounds.empty() ? default_bounds : range_partition.bounds;

  shared_ptr<KuduSession> session(client_->NewSession());
  session->SetTimeoutMillis(10000);
  RETURN_NOT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  int count = 0;
  for (const auto& bound : bounds) {
    for (int32_t i = bound.first[0]; i < bound.second[0]; i++) {
      gscoped_ptr<KuduInsert> insert(table_->NewInsert());
      tools::GenerateDataForRow(table_->schema(), i, &random_, insert->mutable_row());
      inserted_rows_.push_back(new KuduPartialRow(*insert->mutable_row()));
      RETURN_NOT_OK(session->Apply(insert.release()));
      count++;

      if (i > 0 && i % 1000 == 0) {
        RETURN_NOT_OK(session->Flush());
      }
    }
  }

  RETURN_NOT_OK(session->Flush());
  *row_count = count;
  return Status::OK();
}

void FlexPartitioningITest::CheckScanWithColumnPredicate(Slice col_name, int lower, int upper) {
  KuduScanner scanner(table_.get());
  scanner.SetTimeoutMillis(60000);
  CHECK_OK(scanner.AddConjunctPredicate(table_->NewComparisonPredicate(
      col_name, KuduPredicate::GREATER_EQUAL, KuduValue::FromInt(lower))));
  CHECK_OK(scanner.AddConjunctPredicate(table_->NewComparisonPredicate(
      col_name, KuduPredicate::LESS_EQUAL, KuduValue::FromInt(upper))));

  vector<string> rows;
  ScanToStrings(&scanner, &rows);
  std::sort(rows.begin(), rows.end());

  // Manually evaluate the predicate against the data we think we inserted.
  vector<string> expected_rows;
  for (const KuduPartialRow* row : inserted_rows_) {
    int32_t val;
    CHECK_OK(row->GetInt32(col_name, &val));
    if (val >= lower && val <= upper) {
      expected_rows.push_back("(" + row->ToString() + ")");
    }
  }
  std::sort(expected_rows.begin(), expected_rows.end());

  ASSERT_EQ(expected_rows.size(), rows.size());
  ASSERT_EQ(expected_rows, rows);
}

void FlexPartitioningITest::CheckPKRangeScan(int lower, int upper) {
  KuduScanner scanner(table_.get());
  scanner.SetTimeoutMillis(60000);
  ASSERT_OK(scanner.AddLowerBound(*inserted_rows_[lower]));
  ASSERT_OK(scanner.AddExclusiveUpperBound(*inserted_rows_[upper]));
  vector<string> rows;
  ScanToStrings(&scanner, &rows);
  std::sort(rows.begin(), rows.end());

  vector<string> expected_rows;
  for (int i = lower; i < upper; i++) {
    expected_rows.push_back("(" + inserted_rows_[i]->ToString() + ")");
  }
  std::sort(expected_rows.begin(), expected_rows.end());

  ASSERT_EQ(rows.size(), expected_rows.size());
  ASSERT_EQ(rows, expected_rows);
}

void FlexPartitioningITest::CheckPartitionKeyRangeScan() {
  master::GetTableLocationsResponsePB table_locations;
  ASSERT_OK(GetTableLocations(cluster_->master_proxy(),
                    table_->name(),
                    MonoDelta::FromSeconds(32),
                    &table_locations));

  vector<string> rows;

  for (const master::TabletLocationsPB& tablet_locations :
                table_locations.tablet_locations()) {

    string partition_key_start = tablet_locations.partition().partition_key_start();
    string partition_key_end = tablet_locations.partition().partition_key_end();

    KuduScanner scanner(table_.get());
    scanner.SetTimeoutMillis(60000);
    ASSERT_OK(scanner.AddLowerBoundPartitionKeyRaw(partition_key_start));
    ASSERT_OK(scanner.AddExclusiveUpperBoundPartitionKeyRaw(partition_key_end));
    ScanToStrings(&scanner, &rows);
  }
  std::sort(rows.begin(), rows.end());

  vector<string> expected_rows;
  for (KuduPartialRow* row : inserted_rows_) {
    expected_rows.push_back("(" + row->ToString() + ")");
  }
  std::sort(expected_rows.begin(), expected_rows.end());

  ASSERT_EQ(rows.size(), expected_rows.size());
  ASSERT_EQ(rows, expected_rows);
}

void FlexPartitioningITest::CheckPartitionKeyRangeScanWithPKRange(int lower, int upper) {
  master::GetTableLocationsResponsePB table_locations;
  ASSERT_OK(GetTableLocations(cluster_->master_proxy(),
                    table_->name(),
                    MonoDelta::FromSeconds(32),
                    &table_locations));

  vector<string> rows;

  for (const master::TabletLocationsPB& tablet_locations :
                table_locations.tablet_locations()) {

    string partition_key_start = tablet_locations.partition().partition_key_start();
    string partition_key_end = tablet_locations.partition().partition_key_end();

    KuduScanner scanner(table_.get());
    scanner.SetTimeoutMillis(60000);
    ASSERT_OK(scanner.AddLowerBoundPartitionKeyRaw(partition_key_start));
    ASSERT_OK(scanner.AddExclusiveUpperBoundPartitionKeyRaw(partition_key_end));
    ASSERT_OK(scanner.AddLowerBound(*inserted_rows_[lower]));
    ASSERT_OK(scanner.AddExclusiveUpperBound(*inserted_rows_[upper]));
    ScanToStrings(&scanner, &rows);
  }
  std::sort(rows.begin(), rows.end());

  vector<string> expected_rows;
  for (int i = lower; i < upper; i++) {
    expected_rows.push_back("(" + inserted_rows_[i]->ToString() + ")");
  }
  std::sort(expected_rows.begin(), expected_rows.end());

  ASSERT_EQ(rows.size(), expected_rows.size());
  ASSERT_EQ(rows, expected_rows);
}

void FlexPartitioningITest::InsertAndVerifyScans(const RangePartitionOptions& range_partition) {
  int row_count;
  ASSERT_OK(InsertRows(range_partition, &row_count));

  // First, ensure that we get back the same number we put in.
  {
    vector<string> rows;
    ScanTableToStrings(table_.get(), &rows);
    std::sort(rows.begin(), rows.end());
    ASSERT_EQ(row_count, rows.size());
  }

  // Perform some scans with predicates.

  // 1) Various predicates on 'c0', which has non-random data.
  // We concentrate around the value '500' since there is a split point
  // there.
  NO_FATALS(CheckScanWithColumnPredicate("c0", 100, 120));
  NO_FATALS(CheckScanWithColumnPredicate("c0", 490, 610));
  NO_FATALS(CheckScanWithColumnPredicate("c0", 499, 499));
  NO_FATALS(CheckScanWithColumnPredicate("c0", 500, 500));
  NO_FATALS(CheckScanWithColumnPredicate("c0", 501, 501));
  NO_FATALS(CheckScanWithColumnPredicate("c0", 499, 501));
  NO_FATALS(CheckScanWithColumnPredicate("c0", 499, 500));
  NO_FATALS(CheckScanWithColumnPredicate("c0", 500, 501));

  // 2) Random range predicates on the other columns, which are random ints.
  for (int col_idx = 1; col_idx < table_->schema().num_columns(); col_idx++) {
    SCOPED_TRACE(col_idx);
    for (int i = 0; i < 10; i++) {
      int32_t lower = random_.Next32();
      int32_t upper = random_.Next32();
      if (upper < lower) {
        std::swap(lower, upper);
      }

      NO_FATALS(CheckScanWithColumnPredicate(table_->schema().Column(col_idx).name(),
                                             lower, upper));
    }
  }

  // 3) Use the "primary key range" API.
  {
    NO_FATALS(CheckPKRangeScan(100, 120));
    NO_FATALS(CheckPKRangeScan(490, 610));
    NO_FATALS(CheckPKRangeScan(499, 499));
    NO_FATALS(CheckPKRangeScan(500, 500));
    NO_FATALS(CheckPKRangeScan(501, 501));
    NO_FATALS(CheckPKRangeScan(499, 501));
    NO_FATALS(CheckPKRangeScan(499, 500));
    NO_FATALS(CheckPKRangeScan(500, 501));
  }

  // 4) Use the Per-tablet "partition key range" API.
  {
    NO_FATALS(CheckPartitionKeyRangeScan());
  }

  // 5) Use the Per-tablet "partition key range" API with primary key range.
  {
    NO_FATALS(CheckPartitionKeyRangeScanWithPKRange(100, 120));
    NO_FATALS(CheckPartitionKeyRangeScanWithPKRange(200, 400));
    NO_FATALS(CheckPartitionKeyRangeScanWithPKRange(490, 610));
    NO_FATALS(CheckPartitionKeyRangeScanWithPKRange(499, 499));
    NO_FATALS(CheckPartitionKeyRangeScanWithPKRange(500, 500));
    NO_FATALS(CheckPartitionKeyRangeScanWithPKRange(501, 501));
    NO_FATALS(CheckPartitionKeyRangeScanWithPKRange(499, 501));
    NO_FATALS(CheckPartitionKeyRangeScanWithPKRange(499, 500));
    NO_FATALS(CheckPartitionKeyRangeScanWithPKRange(500, 501));
    NO_FATALS(CheckPartitionKeyRangeScanWithPKRange(650, 700));
    NO_FATALS(CheckPartitionKeyRangeScanWithPKRange(700, 800));
  }
}

TEST_F(FlexPartitioningITest, TestFlexPartitioning) {
  vector<vector<HashPartitionOptions>> hash_options {
    // No hash partitioning
    {},
    // HASH (c1) INTO 4 BUCKETS
    { HashPartitionOptions({ "c1" }, 4) },
    // HASH (c0, c1) INTO 3 BUCKETS
    { HashPartitionOptions({ "c0", "c1" }, 3) },
    // HASH (c1, c0) INTO 3 BUCKETS, HASH (c2) INTO 3 BUCKETS
    { HashPartitionOptions({ "c1", "c0" }, 3),
      HashPartitionOptions({ "c2" }, 3) },
    // HASH (c2) INTO 2 BUCKETS, HASH (c1) INTO 2 BUCKETS, HASH (c0) INTO 2 BUCKETS
    { HashPartitionOptions({ "c2" }, 2),
      HashPartitionOptions({ "c1" }, 2),
      HashPartitionOptions({ "c0" }, 2) },
  };

  vector<RangePartitionOptions> range_options {
    // No range partitioning
    RangePartitionOptions({}, {}, {}),
    // RANGE (c0)
    RangePartitionOptions({ "c0" }, { }, { }),
    // RANGE (c0) SPLIT ROWS (500)
    RangePartitionOptions({ "c0" }, { { 500 } }, { }),
    // RANGE (c2, c1) SPLIT ROWS (500, 0), (500, 500), (1000, 0)
    RangePartitionOptions({ "c2", "c1" }, { { 500, 0 }, { 500, 500 }, { 1000, 0 } }, { }),
    // RANGE (c0) BOUNDS ((0), (500)), ((500), (1000))
    RangePartitionOptions({ "c0" }, { }, { { { 0 }, { 500 } }, { { 500 }, { 1000 } } }),
    // RANGE (c0) SPLIT ROWS (500) BOUNDS ((0), (1000))
    RangePartitionOptions({ "c0" }, { }, { { { 0 }, { 500 } }, { { 500 }, { 1000 } } }),
    // RANGE (c0, c1) SPLIT ROWS (500), (2001), (2500), (2999)
    //                BOUNDS ((0), (1000)), ((2000), (3000))
    RangePartitionOptions({ "c0", "c1" }, { { 500 }, { 2001 }, { 2500 }, { 2999 } },
                          { { { 0 }, { 1000 } }, { { 2000 }, { 3000 } } }),
  };

  for (const auto& hash_option : hash_options) {
    for (const auto& range_option: range_options) {
      SCOPED_TRACE(PartitionOptionsToString(hash_option, range_option));
      NO_FATALS(TestPartitionOptions(hash_option, range_option));
    }
  }
}
} // namespace itest
} // namespace kudu
