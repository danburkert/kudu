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

#pragma once

#include <map>
#include <string>
#include <vector>

#include <thrift/protocol/TBinaryProtocol.h>

#include "kudu/common/schema.h"
#include "kudu/hms/ThriftHiveMetastore.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"

namespace hive = Apache::Hadoop::Hive;

namespace kudu {
namespace hms {

// A client for the Hive MetaStore.
//
// All operations are synchronous, and may block.
class HmsClient {
 public:

  static const char* const kKuduTableIdKey;
  static const char* const kKuduMasterAddrsKey;
  static const char* const kKuduStorageHandler;

  static const char* const kTransactionalEventListeners;
  static const char* const kDbNotificationListener;
  static const char* const kKuduMetastorePlugin;

  explicit HmsClient(const HostPort& hms_address);
  ~HmsClient();

  // Starts the HMS client.
  //
  // This method will open a synchronous TCP connection to the HMS. If the HMS
  // can not be reached, an error is returned.
  //
  // Must be called before any subsequent operations using the client.
  Status Start() WARN_UNUSED_RESULT;

  // Stops the HMS client.
  //
  // This is optional; if not called the destructor will stop the client.
  Status Stop() WARN_UNUSED_RESULT;

  // Creates a new database in the HMS.
  //
  // If a database already exists by the same name an AlreadyPresent status is
  // returned.
  Status CreateDatabase(const hive::Database& db) WARN_UNUSED_RESULT;

  // Drops a database in the HMS.
  //
  // If 'cascade' is true, tables in the database will automatically be dropped
  // (this is the default in HiveQL). If 'cascade' is false, the operation will
  // return IllegalState if the database contains tables.
  //
  // If 'delete_data' is true, Hive will automatically delete table partitions
  // of dropped HDFS tables (this is the HiveSQL default).
  Status DropDatabase(const std::string& database_name,
                      bool cascade = false,
                      bool delete_data = true) WARN_UNUSED_RESULT;

  // Returns all HMS databases.
  Status GetAllDatabases(std::vector<std::string>* databases) WARN_UNUSED_RESULT;

  // Retrieves a database from the HMS.
  Status GetDatabase(const std::string& pattern, hive::Database* database) WARN_UNUSED_RESULT;

  // Creates a table in the HMS.
  Status CreateTable(const std::string& database_name,
                     const std::string& table_name,
                     const std::string& table_id) WARN_UNUSED_RESULT;

  // Rename a table in the HMS.
  Status RenameTable(const std::string& old_database_name,
                     const std::string& old_table_name,
                     const std::string& new_database_name,
                     const std::string& new_table_name,
                     const std::string& table_id) WARN_UNUSED_RESULT;

  // Drops a Kudu table in the HMS.
  Status DropTable(const std::string& database_name,
                   const std::string& table_name,
                   const std::string& table_id) WARN_UNUSED_RESULT;

  // Retrieves all tables in an HMS database.
  Status GetAllTables(const std::string& database,
                      std::vector<std::string>* tables) WARN_UNUSED_RESULT;

  // Retrieves an HMS table metadata.
  Status GetTable(const std::string& database,
                  const std::string& table,
                  hive::Table* tbl) WARN_UNUSED_RESULT;

  // Retrieves a the current HMS notification event ID.
  Status GetCurrentNotificationEventId(int64_t* event_id) WARN_UNUSED_RESULT;

  // Retrieves HMS notification log events, beginning after 'last_event_id'.
  Status GetNotificationEvents(int64_t last_event_id,
                               int32_t max_events,
                               std::vector<hive::NotificationEvent>* events) WARN_UNUSED_RESULT;

 private:
  hive::ThriftHiveMetastoreClient client_;
};

} // namespace hms
} // namespace kudu
