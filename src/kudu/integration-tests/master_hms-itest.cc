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
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client-test-util.h"
#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/gutil/port.h"
#include "kudu/hms/hms_client.h"
#include "kudu/master/sys_catalog.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {

using client::KuduClient;
using client::KuduClientBuilder;
using client::KuduColumnSchema;
using client::KuduSchema;
using client::KuduSchemaBuilder;
using client::KuduTable;
using client::KuduTableAlterer;
using client::KuduTableCreator;
using client::sp::shared_ptr;
using cluster::ExternalMiniCluster;
using cluster::ExternalMiniClusterOptions;
using hms::HmsClient;
using std::find;
using std::string;
using std::unique_ptr;
using std::vector;

// Test Master <-> HMS catalog synchronization.
class MasterHmsTest : public KuduTest {

 public:

  MasterHmsTest() = default;

  void SetUp() override {
    KuduTest::SetUp();

    ExternalMiniClusterOptions opts;
    opts.enable_hive_metastore = true;
    opts.num_masters = 1;
    opts.num_tablet_servers = 1;
    cluster_.reset(new ExternalMiniCluster(opts));
    ASSERT_OK(cluster_->Start());

    hms_client_.reset(new HmsClient(cluster_->hms()->address()));
    ASSERT_OK(hms_client_->Start());

    KuduClientBuilder builder;
    ASSERT_OK(cluster_->CreateClient(&builder, &client_));
  }

  void TearDown() override {
    ASSERT_OK(hms_client_->Stop());
    cluster_->Shutdown();
    KuduTest::TearDown();
  }

  Status CreateTable(string table_name) {
    KuduSchema schema;
    KuduSchemaBuilder b;
    b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("int_val")->Type(KuduColumnSchema::INT32)->NotNull();
    b.AddColumn("string_val")->Type(KuduColumnSchema::STRING)->NotNull();
    CHECK_OK(b.Build(&schema));
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    return table_creator->table_name(std::move(table_name))
                         .schema(&schema)
                         .num_replicas(1)
                         .set_range_partition_columns({ "key" })
                         .Create();
  }

  Status HmsRenameTable(const std::string& database_name,
                        const std::string& old_table_name,
                        const std::string& new_table_name) {
    // The HMS doesn't have a rename table API. Instead it offers the more
    // general AlterTable API, which requires the entire set of table fields to be
    // set. Since we don't know these fields during a simple rename operation, we
    // have to look them up.
    hive::Table table;
    RETURN_NOT_OK(hms_client_->GetTable(database_name, old_table_name, &table));
    table.tableName = new_table_name;
    return hms_client_->AlterTable(database_name, old_table_name, table);
  }

 protected:

  unique_ptr<HmsClient> hms_client_;

  unique_ptr<ExternalMiniCluster> cluster_;
  shared_ptr<KuduClient> client_;
};

TEST_F(MasterHmsTest, TestTableLifecycle) {
  const char* table_name = "test.table_lifecycle";
  const char* hms_database_name = "test";
  const char* hms_table_name = "table_lifecycle";

  Status s = CreateTable(table_name);
  ASSERT_FALSE(s.ok());
  ASSERT_STR_CONTAINS(s.ToString(), "InvalidObjectException");

  hive::Database db;
  db.name = hms_database_name;
  ASSERT_OK(hms_client_->CreateDatabase(db));

  ASSERT_OK(CreateTable(table_name));

  vector<string> tables;
  ASSERT_OK(hms_client_->GetAllTables(hms_database_name, &tables));
  ASSERT_TRUE(find(tables.begin(), tables.end(), hms_table_name) != tables.end())
      << "tables: '" << tables << "'";

  unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(table_name));
  table_name = "default.altered_table";
  hms_database_name = "default";
  hms_table_name = "altered_table";
  ASSERT_OK(table_alterer->RenameTo(table_name)->Alter());

  tables.clear();
  ASSERT_OK(hms_client_->GetAllTables(hms_database_name, &tables));
  ASSERT_TRUE(find(tables.begin(), tables.end(), hms_table_name) != tables.end())
      << "tables: '" << tables << "'";

  ASSERT_OK(client_->DeleteTable(table_name));

  tables.clear();
  ASSERT_OK(hms_client_->GetAllTables(hms_database_name, &tables));
  ASSERT_EQ(0, tables.size()) << "tables: '" << tables << "'";
}

TEST_F(MasterHmsTest, TestHmsNotificationLogFollower) {
  const char* table_name = "default.notification_log_follower";
  const char* hms_database_name = "default";
  const char* hms_table_name = "notification_log_follower";

  ASSERT_OK(CreateTable(table_name));

  vector<string> tables;
  ASSERT_OK(hms_client_->GetAllTables(hms_database_name, &tables));
  ASSERT_TRUE(find(tables.begin(), tables.end(), hms_table_name) != tables.end())
      << "tables: '" << tables << "'";

  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(table_name, &table));
  ASSERT_EQ(0, CountTableRows(table.get()));

  ASSERT_OK(HmsRenameTable(hms_database_name, hms_table_name, "new_table_name"));

  ASSERT_EVENTUALLY([&] {
      bool exists;
      ASSERT_OK(client_->TableExists(table_name, &exists));
      ASSERT_FALSE(exists);
      ASSERT_OK(client_->TableExists("default.new_table_name", &exists));
      ASSERT_TRUE(exists);
  });

  hive::EnvironmentContext env_ctx;
  env_ctx.__set_properties({ make_pair(hms::HmsClient::kKuduTableIdKey, table->id()) });
  ASSERT_OK(hms_client_->DropTableWithContext(hms_database_name, "new_table_name", env_ctx));

  ASSERT_EVENTUALLY([&] {
      bool exists;
      ASSERT_OK(client_->TableExists(table_name, &exists));
      ASSERT_FALSE(exists);
  });
}

} // namespace kudu
