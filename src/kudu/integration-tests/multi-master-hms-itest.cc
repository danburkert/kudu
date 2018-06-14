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

#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <glog/stl_logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/common/common.pb.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/hms/hive_metastore_types.h"
#include "kudu/hms/hms_client.h"
#include "kudu/hms/mini_hms.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/util/decimal_util.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {

using client::KuduColumnSchema;
using client::KuduSchema;
using client::KuduSchemaBuilder;
using client::KuduTable;
using client::KuduTableAlterer;
using client::KuduTableCreator;
using client::sp::shared_ptr;
using cluster::ExternalMiniClusterOptions;
using hms::HmsClient;
using hms::HmsClientOptions;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

// Test Master <-> HMS catalog synchronization.
class MultiMasterHmsTest : public ExternalMiniClusterITestBase {
 public:

  void SetUp() override {
    ExternalMiniClusterITestBase::SetUp();

    ExternalMiniClusterOptions opts;
    opts.hms_mode = HmsMode::ENABLE_METASTORE_INTEGRATION;
    opts.num_masters = 3;
    opts.num_tablet_servers = 1;
    // Tune down the notification log poll period in order to speed up catalog convergence.
    opts.extra_master_flags.emplace_back("--hive_metastore_notification_log_poll_period_seconds=1");
    StartClusterWithOpts(std::move(opts));

    hms_client_.reset(new HmsClient(cluster_->hms()->address(), HmsClientOptions()));
    ASSERT_OK(hms_client_->Start());
  }

  void TearDown() override {
    ASSERT_OK(hms_client_->Stop());
    ExternalMiniClusterITestBase::TearDown();
  }

  Status StopHms() {
    RETURN_NOT_OK(hms_client_->Stop());
    RETURN_NOT_OK(cluster_->hms()->Stop());
    return Status::OK();
  }

  Status StartHms() {
    RETURN_NOT_OK(cluster_->hms()->Start());
    RETURN_NOT_OK(hms_client_->Start());
    return Status::OK();
  }

  Status CreateDatabase(const string& database_name) {
    hive::Database db;
    db.name = database_name;
    RETURN_NOT_OK(hms_client_->CreateDatabase(db));
    // Sanity check that the DB is created.
    RETURN_NOT_OK(hms_client_->GetDatabase(database_name, &db));
    return Status::OK();
  }

  Status CreateKuduTable(const string& database_name, const string& table_name) {
    // Get coverage of all column types.
    KuduSchema schema;
    KuduSchemaBuilder b;
    b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("int8_val")->Type(KuduColumnSchema::INT8);
    b.AddColumn("int16_val")->Type(KuduColumnSchema::INT16);
    b.AddColumn("int32_val")->Type(KuduColumnSchema::INT32);
    b.AddColumn("int64_val")->Type(KuduColumnSchema::INT64);
    b.AddColumn("timestamp_val")->Type(KuduColumnSchema::UNIXTIME_MICROS);
    b.AddColumn("string_val")->Type(KuduColumnSchema::STRING);
    b.AddColumn("bool_val")->Type(KuduColumnSchema::BOOL);
    b.AddColumn("float_val")->Type(KuduColumnSchema::FLOAT);
    b.AddColumn("double_val")->Type(KuduColumnSchema::DOUBLE);
    b.AddColumn("binary_val")->Type(KuduColumnSchema::BINARY);
    b.AddColumn("decimal32_val")->Type(KuduColumnSchema::DECIMAL)
        ->Precision(kMaxDecimal32Precision);
    b.AddColumn("decimal64_val")->Type(KuduColumnSchema::DECIMAL)
        ->Precision(kMaxDecimal64Precision);
    b.AddColumn("decimal128_val")->Type(KuduColumnSchema::DECIMAL)
        ->Precision(kMaxDecimal128Precision);

    RETURN_NOT_OK(b.Build(&schema));
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    return table_creator->table_name(Substitute("$0.$1", database_name, table_name))
                         .schema(&schema)
                         .num_replicas(1)
                         .set_range_partition_columns({ "key" })
                         .Create();
  }

  // Rename a table entry in the HMS catalog.
  Status RenameHmsTable(const string& database_name,
                        const string& old_table_name,
                        const string& new_table_name) {
    // The HMS doesn't have a rename table API. Instead it offers the more
    // general AlterTable API, which requires the entire set of table fields to be
    // set. Since we don't know these fields during a simple rename operation, we
    // have to look them up.
    hive::Table table;
    RETURN_NOT_OK(hms_client_->GetTable(database_name, old_table_name, &table));
    table.tableName = new_table_name;
    return hms_client_->AlterTable(database_name, old_table_name, table);
  }

  // Drop all columns from a Kudu HMS table entry.
  Status AlterHmsTableDropColumns(const string& database_name, const string& table_name) {
    hive::Table table;
    RETURN_NOT_OK(hms_client_->GetTable(database_name, table_name, &table));
    table.sd.cols.clear();

    // The KuduMetastorePlugin only allows the master to alter the columns in a
    // Kudu table, so we pretend to be the master.
    hive::EnvironmentContext env_ctx;
    env_ctx.__set_properties({ std::make_pair(hms::HmsClient::kKuduMasterEventKey, "true") });
    RETURN_NOT_OK(hms_client_->AlterTable(database_name, table_name, table, env_ctx));
    return Status::OK();
  }

  // Checks that the Kudu table schema and the HMS table entry in their
  // respective catalogs are synchronized for a particular table.
  void CheckTable(const string& database_name, const string& table_name) {
    SCOPED_TRACE(Substitute("Checking table $0.$1", database_name, table_name));
    shared_ptr<KuduTable> table;
    ASSERT_OK(client_->OpenTable(Substitute("$0.$1", database_name, table_name), &table));
    KuduSchema schema = table->schema();

    hive::Table hms_table;
    ASSERT_OK(hms_client_->GetTable(database_name, table_name, &hms_table));

    ASSERT_EQ(schema.num_columns(), hms_table.sd.cols.size());
    for (int idx = 0; idx < schema.num_columns(); idx++) {
      ASSERT_EQ(schema.Column(idx).name(), hms_table.sd.cols[idx].name);
    }
    ASSERT_EQ(table->id(), hms_table.parameters[hms::HmsClient::kKuduTableIdKey]);
    ASSERT_EQ(HostPort::ToCommaSeparatedString(cluster_->master_rpc_addrs()),
              hms_table.parameters[hms::HmsClient::kKuduMasterAddrsKey]);
    ASSERT_EQ(hms::HmsClient::kKuduStorageHandler,
              hms_table.parameters[hms::HmsClient::kStorageHandlerKey]);
  }

  // Checks that a table does not exist in the Kudu and HMS catalogs.
  void CheckTableDoesNotExist(const string& database_name, const string& table_name) {
    SCOPED_TRACE(Substitute("Checking table $0.$1 does not exist", database_name, table_name));

    shared_ptr<KuduTable> table;
    Status s = client_->OpenTable(Substitute("$0.$1", database_name, table_name), &table);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();

    hive::Table hms_table;
    s = hms_client_->GetTable(database_name, table_name, &hms_table);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  }

  static hive::EnvironmentContext MasterEnvCtx() {
    hive::EnvironmentContext env_ctx;
    env_ctx.__set_properties({ std::make_pair(hms::HmsClient::kKuduMasterEventKey, "true") });
    return env_ctx;
  }

 protected:

  unique_ptr<HmsClient> hms_client_;
};

TEST_F(MultiMasterHmsTest, TestDropWithoutQuorum) {
  const char* hms_database_name = "create_db";
  const char* hms_table_name = "table";
  string table_name = Substitute("$0.$1", hms_database_name, hms_table_name);

  // Attempt to create the table before the database is created.
  Status s = CreateKuduTable(hms_database_name, hms_table_name);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();

  ASSERT_OK(CreateDatabase(hms_database_name));

  // Create a table entry with the name.
  hive::Table hms_table;
  hms_table.dbName = hms_database_name;
  hms_table.tableName = hms_table_name;
  ASSERT_OK(hms_client_->CreateTable(hms_table));

  // Attempt to create a Kudu table with the same name.
  s = CreateKuduTable(hms_database_name, hms_table_name);
  ASSERT_TRUE(s.IsAlreadyPresent()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "table already exists");

  // Attempt to create a Kudu table to an invalid table name.
  s = CreateKuduTable(hms_database_name, "☃");
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "☃ is not a valid object name");

  // Drop the HMS entry and create the table through Kudu.
  ASSERT_OK(hms_client_->DropTable(hms_database_name, hms_table_name));
  ASSERT_OK(CreateKuduTable(hms_database_name, hms_table_name));
  NO_FATALS(CheckTable(hms_database_name, hms_table_name));

  // Shutdown the HMS and try to create a table.
  ASSERT_OK(StopHms());

  s = CreateKuduTable(hms_database_name, "foo");
  ASSERT_TRUE(s.IsNetworkError()) << s.ToString();

  // Start the HMS and try again.
  ASSERT_OK(StartHms());
  ASSERT_EVENTUALLY([&] {
    // HmsCatalog throttles reconnections, so it's necessary to wait out the backoff.
    ASSERT_OK(CreateKuduTable(hms_database_name, "foo"));
  });
  NO_FATALS(CheckTable(hms_database_name, "foo"));
}
} // namespace kudu
