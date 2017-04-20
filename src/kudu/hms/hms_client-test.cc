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

#include <string>
#include <vector>

// See https://stackoverflow.com/questions/1156003/c-namespace-collision-with-gtest-and-boost
#if !defined(_LIBCPP_VERSION)
#define GTEST_USE_OWN_TR1_TUPLE 0
#endif

#include <glog/logging.h>
#include <glog/stl_logging.h>

#include "kudu/gutil/stl_util.h"
#include "kudu/hms/ThriftHiveMetastore.h"
#include "kudu/hms/hive_metastore_constants.h"
#include "kudu/hms/mini_hms.h"
#include "kudu/util/test_util.h"

using std::string;
using std::vector;

namespace kudu {
namespace hms {

class HmsClientTest : public KuduTest {};

TEST_F(HmsClientTest, TestHmsOperations) {
  MiniHms hms;
  ASSERT_OK(hms.Start());

  HmsClient client(hms.address());
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
  ASSERT_TRUE(client.CreateTable(database_name, table_name, "").IsRuntimeError());

  // Create a table.
  ASSERT_OK(client.CreateTable(database_name, table_name, table_id));
  ASSERT_TRUE(client.CreateTable(database_name, table_name, table_id).IsAlreadyPresent());

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
  ASSERT_TRUE(client.RenameTable(database_name, table_name,
                                 database_name, new_table_name,
                                 "bogus-table-id").IsRuntimeError());

  // Rename the table.
  ASSERT_OK(client.RenameTable(database_name, table_name,
                               database_name, new_table_name,
                               table_id));
  // Original table is gone.
  ASSERT_TRUE(client.RenameTable(database_name, table_name,
                                 database_name, new_table_name,
                                 table_id).IsNotFound());

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
  ASSERT_OK(client.CreateTable(database_name, uppercase_table_name, "uppercase-table-id"));

  // Create a table with an illegal utf-8 name.
  ASSERT_TRUE(client.CreateTable(database_name, "☃ sculptures 😉", table_id).IsInvalidArgument());

  // Get all tables.
  vector<string> tables;
  ASSERT_OK(client.GetAllTables(database_name, &tables));
  std::sort(tables.begin(), tables.end());
  EXPECT_EQ(vector<string>({ new_table_name, "my_uppercase_table" }), tables)
      << "Tables: " << tables;

  // Check that the HMS rejects Kudu table drops with a bogus table ID.
  ASSERT_TRUE(client.DropTable(database_name, new_table_name, "bogus-table-id").IsRuntimeError());

  // Drop a table.
  ASSERT_OK(client.DropTable(database_name, new_table_name, table_id));

  // Drop the database.
  ASSERT_TRUE(client.DropDatabase(database_name).IsIllegalState());
  // TODO(HIVE-17008)
  //ASSERT_OK(client.DropDatabase(database_name, true));
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
  LOG(INFO) << events;
  ASSERT_EQ(1, events.size());
  EXPECT_EQ("ALTER_TABLE", events[0].eventType);
  ASSERT_OK(client.Stop());
}

} // namespace hms
} // namespace kudu
