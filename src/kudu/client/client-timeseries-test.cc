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

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <glog/stl_logging.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "kudu/client/client-test-util.h"
#include "kudu/client/client.h"
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/integration-tests/mini_cluster.h"
#include "kudu/master/mini_master.h"
#include "kudu/util/metrics.h"
#include "kudu/util/status.h"
#include "kudu/util/test_util.h"

DECLARE_bool(enable_data_block_fsync);
DECLARE_int32(heartbeat_interval_ms);
DECLARE_int32(scanner_gc_check_interval_us);

using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace client {

using sp::shared_ptr;

// Like ClientTest, but tests operations on a table with a timeseries-like
// schema.
//
// Where ClientTest has a relatively simple schema with a single key column and
// range partitioning, this test's timeseries table has a compound primary key,
// non-default range partitioning, and hash partitioning. The schema is designed
// to test advanced Kudu schema and data distribution features, and is not meant
// to be a model timeseries schema.
//
// CREATE TABLE timeseries
// (host STRING,
//  metric INT32,
//  timestamp TIMESTAMP,
//  value DOUBLE)
// PRIMARY KEY (host, metric, timestamp),
// DISTRIBUTE BY HASH(host) INTO 2 BUCKETS
//               HASH(metric) INTO 2 BUCKETS
//               RANGE(timestamp)
// SPLIT ROWS (('1451606400000000'),  // January 1st 2015
//             ('1420070400000000')); // January 1st 2016
class ClientTimeseriesTest : public KuduTest {
 public:
  ClientTimeseriesTest() {
    KuduSchemaBuilder b;
    b.AddColumn("host")->Type(KuduColumnSchema::STRING)->NotNull();
    b.AddColumn("metric")->Type(KuduColumnSchema::INT32)->NotNull();
    b.AddColumn("timestamp")->Type(KuduColumnSchema::TIMESTAMP)->NotNull();
    b.AddColumn("value")->Type(KuduColumnSchema::DOUBLE)->NotNull();
    b.SetPrimaryKey({ "host", "metric", "timestamp" });
    CHECK_OK(b.Build(&schema_));

    FLAGS_enable_data_block_fsync = false; // Keep unit tests fast.
  }

  virtual void SetUp() override {
    KuduTest::SetUp();

    // Reduce the TS<->Master heartbeat interval
    FLAGS_heartbeat_interval_ms = 10;
    FLAGS_scanner_gc_check_interval_us = 50 * 1000; // 50 milliseconds.

    // Start minicluster and wait for tablet servers to connect to master.
    cluster_.reset(new MiniCluster(env_.get(), MiniClusterOptions()));
    ASSERT_OK(cluster_->Start());
    ASSERT_OK(cluster_->AddTabletServer());
    ASSERT_OK(cluster_->WaitForTabletServerCount(1));

    // Connect to the cluster.
    ASSERT_OK(KuduClientBuilder()
        .add_master_server_addr(cluster_->mini_master()->bound_rpc_addr().ToString())
        .Build(&client_));

    // Create the timeseries table.
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    vector<const KuduPartialRow*> split_rows;
    for (uint64_t timestamp : { 1451606400000000, 1420070400000000 }) {
      KuduPartialRow* row = schema_.NewRow();
      CHECK_OK(row->SetTimestamp("timestamp", timestamp));
      split_rows.push_back(row);
    }

    ASSERT_OK(table_creator->table_name(kTableName)
                            .schema(&schema_)
                            .num_replicas(1)
                            .set_range_partition_columns({ "timestamp" })
                            .add_hash_partitions({ "host" }, 2)
                            .add_hash_partitions({ "metric" }, 2)
                            .split_rows(split_rows)
                            .Create());

    ASSERT_OK(client_->OpenTable(kTableName, &table_));

    // Populate the table
    shared_ptr<KuduSession> session = client_->NewSession();
    ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    session->SetTimeoutMillis(10000);
    for (const string& host : hosts_) {
      for (int32_t metric : metrics_) {
        for (int64_t timestamp : timestamps_) {
          unique_ptr<KuduInsert> insert(table_->NewInsert());
          unique_ptr<KuduPartialRow> row(insert->mutable_row());
          CHECK_OK(row->SetString("host", host));
          CHECK_OK(row->SetInt32("metric", metric));
          CHECK_OK(row->SetTimestamp("timestamp", timestamp));
          CHECK_OK(row->SetDouble("value", timestamp + metric))
          row.release();
          CHECK_OK(session->Apply(insert.release()));
        }
      }
    }
    CHECK_OK(session->Flush());
  }

  virtual void TearDown() override {
    if (cluster_) {
      cluster_->Shutdown();
      cluster_.reset();
    }
    KuduTest::TearDown();
  }

 protected:

  int64_t CountScanResults(KuduScanner* scanner) {
    CHECK_OK(scanner->Open());
    KuduScanBatch batch;
    uint64_t count = 0;
    while (scanner->HasMoreRows()) {
      CHECK_OK(scanner->NextBatch(&batch));
      count += batch.NumRows();
    }
    return count;
  }

  const string kTableName = "timeseries";

  // Prepopulated data. Every (host, metric, timestamp) combination is a row.
  const vector<string> hosts_ = { "host1", "host2", "host3", "host4", "host5" };
  const vector<int32_t> metrics_ = { 100, 200, 300, 400, 500 };
  const vector<int64_t> timestamps_ = {
    1388534400000000, // January 1, 2014
    1420070400000000, // January 1, 2015
    // split
    1420070401000000, // January 1, 2015 + 1 second
    1422748800000000, // February 1, 2015
    // split
    1451606400000000, // January 1, 2016
    1475280000000000  // October 1, 2016
  };

  KuduSchema schema_;
  unique_ptr<MiniCluster> cluster_;
  shared_ptr<KuduClient> client_;
  shared_ptr<KuduTable> table_;
};

TEST_F(ClientTimeseriesTest, TestFullTableScan) {
  KuduScanner scanner(table_.get());
  ASSERT_EQ(hosts_.size() * metrics_.size() * timestamps_.size(),
            CountScanResults(&scanner));
}

// Tests that specifying a partial primary-key as scan bound works as expected.
TEST_F(ClientTimeseriesTest, TestScanPrimaryKeyBound) {
  KuduScanner scanner(table_.get());

  // Set the bound multiple times to test that the most restrictive is kept.
  for (const string& host_bound : { "host2", "host1", "host3" }) {
    unique_ptr<KuduPartialRow> bound(schema_.NewRow());
    ASSERT_OK(bound->SetStringCopy("host", host_bound));
    ASSERT_OK(bound->SetInt32("metric", INT32_MIN));
    ASSERT_OK(bound->SetTimestamp("timestamp", INT64_MIN));
    ASSERT_OK(scanner.AddLowerBound(*bound));
  }

  int64_t expected =
    std::count_if(hosts_.begin(), hosts_.end(), [](const string& host) { return host >= "host3"; })
    * metrics_.size()
    * timestamps_.size();

  ASSERT_EQ(expected, CountScanResults(&scanner));
}

TEST_F(ClientTimeseriesTest, TestScanPredicates) {
  KuduScanner scanner(table_.get());

  CHECK_OK(scanner.AddConjunctPredicate(
      table_->NewComparisonPredicate("host",
                                     KuduPredicate::ComparisonOp::LESS_EQUAL,
                                     KuduValue::CopyString("host2"))));

  CHECK_OK(scanner.AddConjunctPredicate(
        table_->NewComparisonPredicate("metric",
                                       KuduPredicate::ComparisonOp::LESS_EQUAL,
                                       KuduValue::FromInt(300))));

  int64_t expected =
    std::count_if(hosts_.begin(), hosts_.end(), [](const string& host) { return host <= "host2"; })
    * std::count_if(metrics_.begin(), metrics_.end(), [](int64_t metric) { return metric <= 300; })
    * timestamps_.size();

  int64_t results = CountScanResults(&scanner);
  ASSERT_EQ(expected, results);

}

}
}
