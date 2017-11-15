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

#include "kudu/hms/hms_client.h"

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/stl_logging.h> // IWYU pragma: keep
#include <gtest/gtest.h>

#include "kudu/hms/hive_metastore_constants.h"
#include "kudu/hms/hive_metastore_types.h"
#include "kudu/hms/mini_hms.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using boost::optional;
using std::make_pair;
using std::string;
using std::vector;

namespace kudu {
namespace hms {

class HmsClientTest : public KuduTest,
                      public ::testing::WithParamInterface<optional<Protection>> {
 public:

  Status CreateTable(HmsClient* client,
                     const string& database_name,
                     const string& table_name,
                     const string& table_id) {
    hive::Table table;
    table.dbName = database_name;
    table.tableName = table_name;
    table.tableType = "MANAGED_TABLE";

    table.__set_parameters({
        make_pair(HmsClient::kKuduTableIdKey, table_id),
        make_pair(HmsClient::kKuduMasterAddrsKey, string("TODO")),
        make_pair(hive::g_hive_metastore_constants.META_TABLE_STORAGE,
                  HmsClient::kKuduStorageHandler),
    });

    return client->CreateTable(table);
  }

  Status DropTable(HmsClient* client,
                   const string& database_name,
                   const string& table_name,
                   const string& table_id) {
    hive::EnvironmentContext env_ctx;
    env_ctx.__set_properties({ make_pair(HmsClient::kKuduTableIdKey, table_id) });
    return client->DropTableWithContext(database_name, table_name, env_ctx);
  }
};

INSTANTIATE_TEST_CASE_P(ProtectionTypes,
                        HmsClientTest,
                        ::testing::Values(boost::none
                                        , Protection::kIntegrity
// On macos, krb5 has issues repeatedly spinning up new KDCs ('unable to reach
// any KDC in realm KRBTEST.COM, tried 1 KDC'). Integrity protection gives us
// good coverage, so we disable the other variants.
#ifndef __APPLE__
                                        , Protection::kAuthentication
                                        , Protection::kPrivacy
#endif
                                          ));

TEST_P(HmsClientTest, TestHmsOperations) {
  optional<Protection> protection = GetParam();
  MiniKdc kdc;
  MiniHms hms;

  if (protection) {
    ASSERT_OK(kdc.Start());

    string spn = "hive/127.0.0.1";
    string ktpath;
    ASSERT_OK(kdc.CreateServiceKeytab(spn, &ktpath));

    hms.EnableKerberos(kdc.GetEnvVars()["KRB5_CONFIG"],
                       spn,
                       ktpath,
                       *protection);

    ASSERT_OK(kdc.CreateUserPrincipal("alice"));
    ASSERT_OK(kdc.Kinit("alice"));
    ASSERT_OK(kdc.SetKrb5Environment());
  }

  ASSERT_OK(hms.Start());
  HmsClient client(hms.address(), protection ? EnableKerberos::kTrue : EnableKerberos::kFalse);
  ASSERT_OK(client.Start());

  // Create a database.
  string database_name = "my_db";
  hive::Database db;
  db.name = database_name;
  ASSERT_OK(client.CreateDatabase(db));
  ASSERT_TRUE(client.CreateDatabase(db).IsAlreadyPresent());

  // Get all databases.
  vector<string> databases;
  ASSERT_OK(client.GetAllDatabases(&databases));
  std::sort(databases.begin(), databases.end());
  EXPECT_EQ(vector<string>({ "default", database_name }), databases) << "Databases: " << databases;

  // Get a specific database..
  hive::Database my_db;
  ASSERT_OK(client.GetDatabase(database_name, &my_db));
  EXPECT_EQ(database_name, my_db.name) << "my_db: " << my_db;

  string table_name = "my_table";
  string table_id = "table-id";

  // Check that the HMS rejects Kudu tables without a table ID.
  ASSERT_STR_CONTAINS(CreateTable(&client, database_name, table_name, "").ToString(),
                      "Kudu table entry must contain a table ID");

  // Create a table.
  ASSERT_OK(CreateTable(&client, database_name, table_name, table_id));
  ASSERT_TRUE(CreateTable(&client, database_name, table_name, table_id).IsAlreadyPresent());

  // Retrieve a table.
  hive::Table my_table;
  ASSERT_OK(client.GetTable(database_name, table_name, &my_table));
  EXPECT_EQ(database_name, my_table.dbName) << "my_table: " << my_table;
  EXPECT_EQ(table_name, my_table.tableName);
  EXPECT_EQ(table_id, my_table.parameters[HmsClient::kKuduTableIdKey]);
  EXPECT_EQ(HmsClient::kKuduStorageHandler,
            my_table.parameters[hive::g_hive_metastore_constants.META_TABLE_STORAGE]);
  EXPECT_EQ("MANAGED_TABLE", my_table.tableType);

  string new_table_name = "my_altered_table";

  // Renaming the table with an incorrect table ID should fail.
  hive::Table altered_table(my_table);
  altered_table.tableName = new_table_name;
  altered_table.parameters[HmsClient::kKuduTableIdKey] = "bogus-table-id";
  ASSERT_TRUE(client.AlterTable(database_name, table_name, altered_table).IsRemoteError());

  // Rename the table.
  altered_table.parameters[HmsClient::kKuduTableIdKey] = table_id;
  ASSERT_OK(client.AlterTable(database_name, table_name, altered_table));

  // Original table is gone.
  ASSERT_TRUE(client.AlterTable(database_name, table_name, altered_table).IsIllegalState());

  // Check that the altered table's properties are intact.
  hive::Table renamed_table;
  ASSERT_OK(client.GetTable(database_name, new_table_name, &renamed_table));
  EXPECT_EQ(database_name, renamed_table.dbName);
  EXPECT_EQ(new_table_name, renamed_table.tableName);
  EXPECT_EQ(table_id, renamed_table.parameters[HmsClient::kKuduTableIdKey]);
  EXPECT_EQ(HmsClient::kKuduStorageHandler,
            renamed_table.parameters[hive::g_hive_metastore_constants.META_TABLE_STORAGE]);
  EXPECT_EQ("MANAGED_TABLE", renamed_table.tableType);

  // Create a table with an uppercase name.
  string uppercase_table_name = "my_UPPERCASE_Table";
  ASSERT_OK(CreateTable(&client, database_name, uppercase_table_name, "uppercase-table-id"));

  // Create a table with an illegal utf-8 name.
  ASSERT_TRUE(CreateTable(&client, database_name, "☃ sculptures 😉", table_id).IsInvalidArgument());

  // Get all tables.
  vector<string> tables;
  ASSERT_OK(client.GetAllTables(database_name, &tables));
  std::sort(tables.begin(), tables.end());
  EXPECT_EQ(vector<string>({ new_table_name, "my_uppercase_table" }), tables)
      << "Tables: " << tables;

  // Check that the HMS rejects Kudu table drops with a bogus table ID.
  ASSERT_TRUE(DropTable(&client, database_name, new_table_name, "bogus-table-id").IsRemoteError());
  // Check that the HMS rejects non-existent table drops.
  ASSERT_TRUE(DropTable(&client, database_name, "foo-bar", "bogus-table-id").IsNotFound());

  // Drop a table.
  ASSERT_OK(DropTable(&client, database_name, new_table_name, table_id));

  // Drop the database.
  ASSERT_TRUE(client.DropDatabase(database_name).IsIllegalState());
  // TODO(HIVE-17008)
  // ASSERT_OK(client.DropDatabase(database_name, Cascade::kTrue));
  // TODO(HIVE-17008)
  // ASSERT_TRUE(client.DropDatabase(database_name).IsNotFound());

  int64_t event_id;
  ASSERT_OK(client.GetCurrentNotificationEventId(&event_id));

  // Retrieve the notification log and spot-check that the results look sensible.
  vector<hive::NotificationEvent> events;
  ASSERT_OK(client.GetNotificationEvents(-1, 100, &events));

  ASSERT_EQ(5, events.size());
  EXPECT_EQ("CREATE_DATABASE", events[0].eventType);
  EXPECT_EQ("CREATE_TABLE", events[1].eventType);
  EXPECT_EQ("ALTER_TABLE", events[2].eventType);
  EXPECT_EQ("CREATE_TABLE", events[3].eventType);
  EXPECT_EQ("DROP_TABLE", events[4].eventType);
  // TODO(HIVE-17008)
  //EXPECT_EQ("DROP_TABLE", events[5].eventType);
  //EXPECT_EQ("DROP_DATABASE", events[6].eventType);

  // Retrieve a specific notification log.
  events.clear();
  ASSERT_OK(client.GetNotificationEvents(2, 1, &events));
  ASSERT_EQ(1, events.size()) << "events: " << events;
  EXPECT_EQ("ALTER_TABLE", events[0].eventType);
  ASSERT_OK(client.Stop());
}

TEST(HmsClientTest, TestDeserializeJsonTable) {
  string json = R"#({"1":{"str":"table_name"},"2":{"str":"database_name"}})#";
  hive::Table table;
  ASSERT_OK(HmsClient::DeserializeJsonTable(json, &table));
  ASSERT_EQ("table_name", table.tableName);
  ASSERT_EQ("database_name", table.dbName);
}

} // namespace hms
} // namespace kudu
