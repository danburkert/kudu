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

#include <boost/shared_ptr.hpp>
#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSocket.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/hms/ThriftHiveMetastore.h"
#include "kudu/hms/hive_metastore_constants.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"

using apache::thrift::protocol::TBinaryProtocol;
using apache::thrift::protocol::TProtocol;
using apache::thrift::protocol::TTransport;
using apache::thrift::TException;
using apache::thrift::transport::TBufferedTransport;
using apache::thrift::transport::TSocket;
using std::string;
using std::vector;

namespace kudu {
namespace hms {

// The entire set of Hive-specific exceptions is defined in
// hive_metastore.thrift. We do not try to handle all of them - TException acts
// as a catch all, as well as default for network errors.
#define HMS_RET_NOT_OK(call, msg) \
  try { \
    (call); \
  } catch (const hive::AlreadyExistsException& e) { \
    return Status::AlreadyPresent((msg), e.what()); \
  } catch (const hive::UnknownDBException& e) { \
    return Status::NotFound((msg), e.what()); \
  } catch (const hive::UnknownTableException& e) { \
    return Status::NotFound((msg), e.what()); \
  } catch (const hive::NoSuchObjectException& e) { \
    return Status::NotFound((msg), e.what()); \
  } catch (const hive::InvalidObjectException& e) { \
    return Status::InvalidArgument((msg), e.what()); \
  } catch (const hive::InvalidOperationException& e) { \
    return Status::IllegalState((msg), e.what()); \
  } catch (const hive::MetaException& e) { \
    return Status::RuntimeError((msg), e.what()); \
  } catch (const TException& e) { \
    return Status::IOError((msg), e.what()); \
  }

const char* const HmsClient::kKuduTableIdKey = "kudu.table_id";
const char* const HmsClient::kKuduMasterAddrsKey = "kudu.master_addresses";
const char* const HmsClient::kKuduStorageHandler = "org.apache.kudu.hive.KuduStorageHandler";

const char* const HmsClient::kTransactionalEventListeners =
  "hive.metastore.transactional.event.listeners";
const char* const HmsClient::kDbNotificationListener =
  "org.apache.hive.hcatalog.listener.DbNotificationListener";
const char* const HmsClient::kKuduMetastorePlugin =
  "org.apache.kudu.hive.metastore.KuduMetastorePlugin";

HmsClient::HmsClient(const HostPort& hms_address)
    : client_(nullptr) {
  boost::shared_ptr<TSocket> socket(new TSocket(hms_address.host(), hms_address.port()));
  boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
  client_ = hive::ThriftHiveMetastoreClient(protocol);
}

HmsClient::~HmsClient() {
  WARN_NOT_OK(Stop(), "failed to shutdown HMS client");
}

Status HmsClient::Start() {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, 1000 /* ms */, "starting HMS client");
  HMS_RET_NOT_OK(client_.getOutputProtocol()->getTransport()->open(),
                 "failed to open Hive MetaStore connection");

  // Immediately after connecting to the HMS, check that it is configured with
  // the required event listeners.
  string event_listener_config;
  HMS_RET_NOT_OK(client_.get_config_value(event_listener_config, kTransactionalEventListeners, ""),
                 "failed to get Hive MetaStore transactional event listener configuration");

  // Parse the set of listeners from the configuration string.
  vector<string> listeners = strings::Split(event_listener_config, ",", strings::SkipWhitespace());
  for (auto& listener : listeners) {
    StripWhiteSpace(&listener);
  }

  for (const auto& required_listener : { kDbNotificationListener, kKuduMetastorePlugin }) {
    if (std::find(listeners.begin(), listeners.end(), required_listener) == listeners.end()) {
      return Status::IllegalState(
          strings::Substitute("Hive Metastore configuration is missing required "
                              "transactional event listener ($0): $1",
                              kTransactionalEventListeners, required_listener));
    }
  }

  return Status::OK();
}

Status HmsClient::Stop() {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, 500 /* ms */, "stopping HMS client");
  HMS_RET_NOT_OK(client_.getInputProtocol()->getTransport()->close(),
                 "failed to close Hive MetaStore connection");
  return Status::OK();
}

Status HmsClient::CreateDatabase(const hive::Database& db) {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, 500 /* ms */, "create HMS database");
  HMS_RET_NOT_OK(client_.create_database(db), "failed to create Hive MetaStore database");
  return Status::OK();
}

Status HmsClient::DropDatabase(const string& database_name,
                               bool cascade,
                               bool delete_data) {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, 500 /* ms */, "drop HMS database");
  HMS_RET_NOT_OK(client_.drop_database(database_name, delete_data, cascade),
                 "failed to drop Hive MetaStore database");
  return Status::OK();
}

Status HmsClient::GetAllDatabases(vector<string>* databases) {
  DCHECK(databases);
  SCOPED_LOG_SLOW_EXECUTION(WARNING, 500 /* ms */, "get all HMS databases");
  HMS_RET_NOT_OK(client_.get_all_databases(*databases),
                 "failed to get Hive MetaStore databases");
  return Status::OK();
}

Status HmsClient::GetDatabase(const string& pattern, hive::Database* database) {
  DCHECK(database);
  SCOPED_LOG_SLOW_EXECUTION(WARNING, 500 /* ms */, "get HMS database");
  HMS_RET_NOT_OK(client_.get_database(*database, pattern),
                 "failed to get Hive MetaStore database");
  return Status::OK();
}

Status HmsClient::CreateTable(const string& database_name,
                              const string& table_name,
                              const string& table_id) {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, 500 /* ms */, "create HMS table");
  hive::Table table;
  table.dbName = database_name;
  table.tableName = table_name;
  table.tableType = "MANAGED_TABLE";

  table.__set_parameters({
      make_pair(kKuduTableIdKey, table_id),
      make_pair(kKuduMasterAddrsKey, string("TODO")),
      make_pair(hive::g_hive_metastore_constants.META_TABLE_STORAGE, kKuduStorageHandler),
  });

  HMS_RET_NOT_OK(client_.create_table(table), "failed to create Hive MetaStore table");
  return Status::OK();
}

Status HmsClient::RenameTable(const string& old_database_name,
                              const string& old_table_name,
                              const string& new_database_name,
                              const string& new_table_name,
                              const string& table_id) {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, 1000 /* ms */, "rename HMS table");
  // The HMS doesn't have a rename table API. Instead it offers the more
  // general AlterTable API, which requires the entire set of table fields to be
  // set. Since we don't know these fields during a simple rename operation, we
  // have to look them up.
  //
  // This introduces a potential race between when the table information is
  // retrieved and the AlterTable call is made. To limit the possibility of
  // inconsistency, we check in the HMS (via the KuduMetastorePlugin) to ensure
  // that the Kudu table ID remains consistent.

  hive::Table table;
  RETURN_NOT_OK(GetTable(old_database_name, old_table_name, &table));

  table.dbName = new_database_name;
  table.tableName = new_table_name;
  table.parameters[kKuduTableIdKey] = table_id;

  HMS_RET_NOT_OK(client_.alter_table(old_database_name, old_table_name, table),
                 "failed to rename Hive MetaStore table");
  return Status::OK();
}

Status HmsClient::DropTable(const string& database_name,
                            const string& table_name,
                            const string& table_id) {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, 500 /* ms */, "drop HMS table");
  hive::EnvironmentContext env_ctx;
  env_ctx.__set_properties({ make_pair(kKuduTableIdKey, table_id) });
  HMS_RET_NOT_OK(client_.drop_table_with_environment_context(database_name, table_name,
                                                             true, env_ctx),
                 "failed to delete Hive MetaStore table");
  return Status::OK();
}

Status HmsClient::GetAllTables(const string& database,
                               vector<string>* tables) {
  DCHECK(tables);
  SCOPED_LOG_SLOW_EXECUTION(WARNING, 500 /* ms */, "get all HMS tables");
  HMS_RET_NOT_OK(client_.get_all_tables(*tables, database),
                 "failed to get Hive MetaStore tables");
  return Status::OK();
}

Status HmsClient::GetTable(const string& database,
                           const string& table,
                           hive::Table* tbl) {
  DCHECK(tbl);
  SCOPED_LOG_SLOW_EXECUTION(WARNING, 500 /* ms */, "get HMS table");
  HMS_RET_NOT_OK(client_.get_table(*tbl, database, table),
                 "failed to get Hive MetaStore table");
  return Status::OK();
}

Status HmsClient::GetCurrentNotificationEventId(int64_t* event_id) {
  DCHECK(event_id);
  SCOPED_LOG_SLOW_EXECUTION(WARNING, 500 /* ms */, "get HMS current notification event ID");
  hive::CurrentNotificationEventId response;
  HMS_RET_NOT_OK(client_.get_current_notificationEventId(response),
                 "failed to get Hive MetaStore current event ID");
  *event_id = response.eventId;
  return Status::OK();
}

Status HmsClient::GetNotificationEvents(int64_t last_event_id,
                                        int32_t max_events,
                                        vector<hive::NotificationEvent>* events) {
  DCHECK(events);
  SCOPED_LOG_SLOW_EXECUTION(WARNING, 500 /* ms */, "get HMS notification events");
  hive::NotificationEventRequest request;
  request.lastEvent = last_event_id;
  request.__set_maxEvents(max_events);
  hive::NotificationEventResponse response;
  HMS_RET_NOT_OK(client_.get_next_notification(response, request),
                 "failed to get Hive MetaStore next notification");
  events->swap(response.events);
  return Status::OK();
}

} // namespace hms
} // namespace kudu
