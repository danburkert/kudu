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

#include <algorithm>
#include <exception>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <vector>

#include <boost/algorithm/string/predicate.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <thrift/TOutput.h>
#include <thrift/Thrift.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TJSONProtocol.h>
#include <thrift/protocol/TProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransport.h>
#include <thrift/transport/TTransportException.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/hms/ThriftHiveMetastore.h"
#include "kudu/hms/hive_metastore_types.h"
#include "kudu/hms/sasl_client_transport.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"

// Default to 100 MiB to match Thrift TSaslTransport.receiveSaslMessage and the
// HMS metastore.server.max.message.size config.
DEFINE_int32(hms_client_max_buf_size, 100 * 1024 * 1024,
             "Maximum size of Hive Metastore objects that can be received by the "
             "HMS client in bytes.");
TAG_FLAG(hms_client_max_buf_size, experimental);
// Note: despite being marked as a runtime flag, the new buf size value will
// only take effect for new HMS clients.
TAG_FLAG(hms_client_max_buf_size, runtime);

using apache::thrift::TException;
using apache::thrift::protocol::TBinaryProtocol;
using apache::thrift::protocol::TJSONProtocol;
using apache::thrift::transport::TBufferedTransport;
using apache::thrift::transport::TMemoryBuffer;
using apache::thrift::transport::TSocket;
using apache::thrift::transport::TTransport;
using apache::thrift::transport::TTransportException;
using std::make_shared;
using std::shared_ptr;
using std::string;
using std::vector;
using strings::Substitute;

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
    return Status::RemoteError((msg), e.what()); \
  } catch (const SaslException& e) { \
    return e.status().CloneAndPrepend((msg)); \
  } catch (const TTransportException& e) { \
    switch (e.getType()) { \
      case TTransportException::TIMED_OUT: return Status::TimedOut((msg), e.what()); \
      case TTransportException::BAD_ARGS: return Status::InvalidArgument((msg), e.what()); \
      case TTransportException::CORRUPTED_DATA: return Status::Corruption((msg), e.what()); \
      default: return Status::NetworkError((msg), e.what()); \
    } \
  } catch (const TException& e) { \
    return Status::IOError((msg), e.what()); \
  } catch (const std::exception& e) { \
    return Status::RuntimeError((msg), e.what()); \
  }

const char* const HmsClient::kLegacyKuduStorageHandler =
  "com.cloudera.kudu.hive.KuduStorageHandler";
const char* const HmsClient::kLegacyKuduTableNameKey = "kudu.table_name";
const char* const HmsClient::kLegacyTablePrefix = "impala::";
const char* const HmsClient::kKuduTableIdKey = "kudu.table_id";
const char* const HmsClient::kKuduMasterAddrsKey = "kudu.master_addresses";
const char* const HmsClient::kKuduMasterEventKey = "kudu.master_event";
const char* const HmsClient::kKuduStorageHandler = "org.apache.kudu.hive.KuduStorageHandler";

const char* const HmsClient::kTransactionalEventListeners =
  "hive.metastore.transactional.event.listeners";
const char* const HmsClient::kDisallowIncompatibleColTypeChanges =
  "hive.metastore.disallow.incompatible.col.type.changes";
const char* const HmsClient::kDbNotificationListener =
  "org.apache.hive.hcatalog.listener.DbNotificationListener";
const char* const HmsClient::kExternalTableKey = "EXTERNAL";
const char* const HmsClient::kStorageHandlerKey = "storage_handler";
const char* const HmsClient::kKuduMetastorePlugin =
  "org.apache.kudu.hive.metastore.KuduMetastorePlugin";

const char* const HmsClient::kManagedTable = "MANAGED_TABLE";
const char* const HmsClient::kExternalTable = "EXTERNAL_TABLE";

const uint16_t HmsClient::kDefaultHmsPort = 9083;

const int kSlowExecutionWarningThresholdMs = 1000;

namespace {
// A logging callback for Thrift.
//
// Normally this would be defined in a more neutral location (e.g. Impala
// defines it in thrift-util.cc), but since Hive is currently Kudu's only user
// of Thrift, it's nice to have the log messsages originate from hms_client.cc.
void ThriftOutputFunction(const char* output) {
  LOG(INFO) << output;
}

class ScopedLog {
 public:
  ScopedLog(string msg) : msg_(std::move(msg)) {
    LOG(INFO) << ">>>>>>>>>>>>>>>>>>>>>>>>>>>>> " << msg_;
  }

  ~ScopedLog() {
    LOG(INFO) << "<<<<<<<<<<<<<<<<<<<<<<<<<<<<< " << msg_;
  }

  string msg_;
};

} // anonymous namespace

HmsClient::HmsClient(const HostPort& hms_address, const HmsClientOptions& options)
      : client_(nullptr) {
  static std::once_flag set_thrift_logging_callback;
  std::call_once(set_thrift_logging_callback, [] {
      apache::thrift::GlobalOutput.setOutputFunction(ThriftOutputFunction);
  });

  auto socket = make_shared<TSocket>(hms_address.host(), hms_address.port());
  socket->setSendTimeout(options.send_timeout.ToMilliseconds());
  socket->setRecvTimeout(options.recv_timeout.ToMilliseconds());
  socket->setConnTimeout(options.conn_timeout.ToMilliseconds());
  shared_ptr<TTransport> transport;

  if (options.enable_kerberos) {
    transport = make_shared<SaslClientTransport>(hms_address.host(),
                                                 std::move(socket),
                                                 FLAGS_hms_client_max_buf_size);
  } else {
    transport = make_shared<TBufferedTransport>(std::move(socket));
  }

  auto protocol = make_shared<TBinaryProtocol>(std::move(transport));
  client_ = hive::ThriftHiveMetastoreClient(std::move(protocol));
}

HmsClient::~HmsClient() {
  WARN_NOT_OK(Stop(), "failed to shutdown HMS client");
}

Status HmsClient::Start() {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "starting HMS client");
  HMS_RET_NOT_OK(client_.getOutputProtocol()->getTransport()->open(),
                 "failed to open Hive Metastore connection");

  // Immediately after connecting to the HMS, check that it is configured with
  // the required event listeners.
  string event_listener_config;
  HMS_RET_NOT_OK(client_.get_config_value(event_listener_config, kTransactionalEventListeners, ""),
                 Substitute("failed to get Hive Metastore $0 configuration",
                            kTransactionalEventListeners));

  // Parse the set of listeners from the configuration string.
  vector<string> listeners = strings::Split(event_listener_config, ",", strings::SkipWhitespace());
  for (auto& listener : listeners) {
    StripWhiteSpace(&listener);
    LOG(INFO) << "Listener: " << listener;
  }

  LOG(INFO) << "num listeners: " << listeners.size();

  for (const auto& required_listener : { kDbNotificationListener, kKuduMetastorePlugin }) {
    if (std::find(listeners.begin(), listeners.end(), required_listener) == listeners.end()) {
      return Status::IllegalState(
          Substitute("Hive Metastore configuration is missing required "
                     "transactional event listener ($0): $1",
                     kTransactionalEventListeners, required_listener));
    }
  }

  // Also check that the HMS is configured to allow changing the type of
  // columns, which is required to support dropping columns. File-based Hive
  // tables handle columns by offset, and removing a column simply shifts all
  // remaining columns down by one. The actual data is not changed, but instead
  // reassigned to the next column. If the HMS is configured to check column
  // type changes it will validate that the existing data matches the shifted
  // column layout. This is overly strict for Kudu, since we handle DDL
  // operations properly.
  //
  // See org.apache.hadoop.hive.metastore.MetaStoreUtils.throwExceptionIfIncompatibleColTypeChange.
  string disallow_incompatible_column_type_changes;
  HMS_RET_NOT_OK(client_.get_config_value(disallow_incompatible_column_type_changes,
                                          kDisallowIncompatibleColTypeChanges,
                                          "false"),
                 Substitute("failed to get Hive Metastore $0 configuration",
                            kDisallowIncompatibleColTypeChanges));

  if (boost::iequals(disallow_incompatible_column_type_changes, "true")) {
    return Status::IllegalState(Substitute(
          "Hive Metastore configuration is invalid: $0 must be set to false",
          kDisallowIncompatibleColTypeChanges));
  }

  return Status::OK();
}

Status HmsClient::Stop() {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "stopping HMS client");
  HMS_RET_NOT_OK(client_.getInputProtocol()->getTransport()->close(),
                 "failed to close Hive Metastore connection");
  return Status::OK();
}

bool HmsClient::IsConnected() {
  return client_.getInputProtocol()->getTransport()->isOpen();
}

Status HmsClient::CreateDatabase(const hive::Database& database) {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "create HMS database");
  HMS_RET_NOT_OK(client_.create_database(database), "failed to create Hive Metastore database");
  return Status::OK();
}

Status HmsClient::DropDatabase(const string& database_name, Cascade cascade) {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "drop HMS database");
  HMS_RET_NOT_OK(client_.drop_database(database_name, true, cascade == Cascade::kTrue),
                 "failed to drop Hive Metastore database");
  return Status::OK();
}

Status HmsClient::GetAllDatabases(vector<string>* databases) {
  DCHECK(databases);
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "get all HMS databases");
  HMS_RET_NOT_OK(client_.get_all_databases(*databases),
                 "failed to get Hive Metastore databases");
  return Status::OK();
}

Status HmsClient::GetDatabase(const string& pattern, hive::Database* database) {
  DCHECK(database);
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "get HMS database");
  HMS_RET_NOT_OK(client_.get_database(*database, pattern),
                 "failed to get Hive Metastore database");
  return Status::OK();
}

Status HmsClient::CreateTable(const hive::Table& table, const hive::EnvironmentContext& env_ctx) {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "create HMS table");
  std::stringstream s;
  s << table;
  auto l = ScopedLog(Substitute("HmsClient::CreateTable: $0", s.str()));
  HMS_RET_NOT_OK(client_.create_table_with_environment_context(table, env_ctx),
                 "failed to create Hive MetaStore table");
  return Status::OK();
}

Status HmsClient::AlterTable(const std::string& database_name,
                             const std::string& table_name,
                             const hive::Table& table,
                             const hive::EnvironmentContext& env_ctx) {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "alter HMS table");
  std::stringstream s;
  s << table;
  auto l = ScopedLog(Substitute("HmsClient::AlterTable: $0.$1: $2", database_name, table_name, s.str()));
  HMS_RET_NOT_OK(client_.alter_table_with_environment_context(database_name, table_name,
                                                              table, env_ctx),
                 "failed to alter Hive MetaStore table");
  return Status::OK();
}

Status HmsClient::DropTable(const string& database_name,
                            const string& table_name,
                            const hive::EnvironmentContext& env_ctx) {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "drop HMS table");
  auto l = ScopedLog(Substitute("HmsClient::DropTable: $0.$1", database_name, table_name));
  HMS_RET_NOT_OK(client_.drop_table_with_environment_context(database_name, table_name,
                                                             true, env_ctx),
                 "failed to drop Hive Metastore table");
  return Status::OK();
}

Status HmsClient::GetAllTables(const string& database_name,
                               vector<string>* tables) {
  DCHECK(tables);
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "get all HMS tables");
  HMS_RET_NOT_OK(client_.get_all_tables(*tables, database_name),
                 "failed to get Hive Metastore tables");
  return Status::OK();
}

Status HmsClient::GetTable(const string& database_name,
                           const string& table_name,
                           hive::Table* table) {
  DCHECK(table);
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "get HMS table");
  auto l = ScopedLog(Substitute("HmsClient::GetTable: $0.$1", database_name, table_name));
  HMS_RET_NOT_OK(client_.get_table(*table, database_name, table_name),
                 "failed to get Hive Metastore table");
  return Status::OK();
}

Status HmsClient::GetCurrentNotificationEventId(int64_t* event_id) {
  DCHECK(event_id);
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs,
                            "get HMS current notification event ID");
  auto l = ScopedLog("HmsClient::GetCurrentNotificationEventId");
  hive::CurrentNotificationEventId response;
  HMS_RET_NOT_OK(client_.get_current_notificationEventId(response),
                 "failed to get Hive Metastore current event ID");
  *event_id = response.eventId;
  return Status::OK();
}

Status HmsClient::GetNotificationEvents(int64_t last_event_id,
                                        int32_t max_events,
                                        vector<hive::NotificationEvent>* events) {
  DCHECK(events);
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs,
                            "get HMS notification events");
  auto l = ScopedLog(Substitute("HmsClient::GetNotificationEvents: last_event_id: $0", last_event_id));
  hive::NotificationEventRequest request;
  request.lastEvent = last_event_id;
  request.__set_maxEvents(max_events);
  hive::NotificationEventResponse response;
  HMS_RET_NOT_OK(client_.get_next_notification(response, request),
                 "failed to get Hive Metastore next notification");
  events->swap(response.events);
  return Status::OK();
}

Status HmsClient::AddPartitions(const string& database_name,
                                const string& table_name,
                                vector<hive::Partition> partitions) {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "add HMS table partitions");
  hive::AddPartitionsRequest request;
  hive::AddPartitionsResult response;

  request.dbName = database_name;
  request.tblName = table_name;
  request.parts = std::move(partitions);

  HMS_RET_NOT_OK(client_.add_partitions_req(response, request),
                 "failed to add Hive Metastore table partitions");
  return Status::OK();
}

Status HmsClient::GetPartitions(const string& database_name,
                                const string& table_name,
                                vector<hive::Partition>* partitions) {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "get HMS table partitions");
  HMS_RET_NOT_OK(client_.get_partitions(*partitions, database_name, table_name, -1),
                 "failed to get Hive Metastore table partitions");
  return Status::OK();
}

Status HmsClient::DeserializeJsonTable(Slice json, hive::Table* table)  {
  shared_ptr<TMemoryBuffer> membuffer(new TMemoryBuffer(json.size()));
  membuffer->write(json.data(), json.size());
  TJSONProtocol protocol(membuffer);
  HMS_RET_NOT_OK(table->read(&protocol), "failed to deserialize JSON table");
  return Status::OK();
}

} // namespace hms
} // namespace kudu
