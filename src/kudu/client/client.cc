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

#include "kudu/client/client.h"

#include <algorithm>
#include <boost/bind.hpp>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "kudu/client/batcher.h"
#include "kudu/client/callbacks.h"
#include "kudu/client/client-internal.h"
#include "kudu/client/client_builder-internal.h"
#include "kudu/client/error-internal.h"
#include "kudu/client/error_collector.h"
#include "kudu/client/meta_cache.h"
#include "kudu/client/row_result.h"
#include "kudu/client/scan_predicate-internal.h"
#include "kudu/client/scan_token-internal.h"
#include "kudu/client/scanner-internal.h"
#include "kudu/client/schema-internal.h"
#include "kudu/client/session-internal.h"
#include "kudu/client/table-internal.h"
#include "kudu/client/table_alterer-internal.h"
#include "kudu/client/table_creator-internal.h"
#include "kudu/client/tablet_server-internal.h"
#include "kudu/client/write_op.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/partition.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.h" // TODO: remove this include - just needed for default port
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/rpc/messenger.h"
#include "kudu/util/logging.h"
#include "kudu/util/net/dns_resolver.h"
#include "kudu/util/version_info.h"

using kudu::master::AlterTableRequestPB;
using kudu::master::AlterTableRequestPB_Step;
using kudu::master::AlterTableResponsePB;
using kudu::master::CreateTableRequestPB;
using kudu::master::CreateTableResponsePB;
using kudu::master::DeleteTableRequestPB;
using kudu::master::DeleteTableResponsePB;
using kudu::master::GetTableSchemaRequestPB;
using kudu::master::GetTableSchemaResponsePB;
using kudu::master::ListTablesRequestPB;
using kudu::master::ListTablesResponsePB;
using kudu::master::ListTabletServersRequestPB;
using kudu::master::ListTabletServersResponsePB;
using kudu::master::ListTabletServersResponsePB_Entry;
using kudu::master::MasterServiceProxy;
using kudu::master::TabletLocationsPB;
using kudu::rpc::Messenger;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;
using kudu::tserver::ScanResponsePB;
using std::set;
using std::string;
using std::vector;

MAKE_ENUM_LIMITS(kudu::client::KuduSession::FlushMode,
                 kudu::client::KuduSession::AUTO_FLUSH_SYNC,
                 kudu::client::KuduSession::MANUAL_FLUSH);

MAKE_ENUM_LIMITS(kudu::client::KuduSession::ExternalConsistencyMode,
                 kudu::client::KuduSession::CLIENT_PROPAGATED,
                 kudu::client::KuduSession::COMMIT_WAIT);

MAKE_ENUM_LIMITS(kudu::client::KuduScanner::ReadMode,
                 kudu::client::KuduScanner::READ_LATEST,
                 kudu::client::KuduScanner::READ_AT_SNAPSHOT);

MAKE_ENUM_LIMITS(kudu::client::KuduScanner::OrderMode,
                 kudu::client::KuduScanner::UNORDERED,
                 kudu::client::KuduScanner::ORDERED);

namespace kudu {
namespace client {

using internal::Batcher;
using internal::ErrorCollector;
using internal::MetaCache;
using internal::Session;
using sp::shared_ptr;

static const char* kProgName = "kudu_client";

// We need to reroute all logging to stderr when the client library is
// loaded. GoogleOnceInit() can do that, but there are multiple entry
// points into the client code, and it'd need to be called in each one.
// So instead, let's use a constructor function.
//
// Should this be restricted to just the exported client build? Probably
// not, as any application using the library probably wants stderr logging
// more than file logging.
__attribute__((constructor))
static void InitializeBasicLogging() {
  InitGoogleLoggingSafeBasic(kProgName);
}

// Adapts between the internal LogSeverity and the client's KuduLogSeverity.
static void LoggingAdapterCB(KuduLoggingCallback* user_cb,
                             LogSeverity severity,
                             const char* filename,
                             int line_number,
                             const struct ::tm* time,
                             const char* message,
                             size_t message_len) {
  KuduLogSeverity client_severity;
  switch (severity) {
    case kudu::SEVERITY_INFO:
      client_severity = SEVERITY_INFO;
      break;
    case kudu::SEVERITY_WARNING:
      client_severity = SEVERITY_WARNING;
      break;
    case kudu::SEVERITY_ERROR:
      client_severity = SEVERITY_ERROR;
      break;
    case kudu::SEVERITY_FATAL:
      client_severity = SEVERITY_FATAL;
      break;
    default:
      LOG(FATAL) << "Unknown Kudu log severity: " << severity;
  }
  user_cb->Run(client_severity, filename, line_number, time,
               message, message_len);
}

void InstallLoggingCallback(KuduLoggingCallback* cb) {
  RegisterLoggingCallback(Bind(&LoggingAdapterCB, Unretained(cb)));
}

void UninstallLoggingCallback() {
  UnregisterLoggingCallback();
}

void SetVerboseLogLevel(int level) {
  FLAGS_v = level;
}

Status SetInternalSignalNumber(int signum) {
  return SetStackTraceSignal(signum);
}

std::string GetShortVersionString() {
  return VersionInfo::GetShortVersionString();
}

std::string GetAllVersionInfo() {
  return VersionInfo::GetAllVersionInfo();
}

////////////////////////////////////////////////////////////
// KuduClientBuilder
////////////////////////////////////////////////////////////

KuduClientBuilder::KuduClientBuilder()
  : data_(new KuduClientBuilder::Data()) {
}

KuduClientBuilder::~KuduClientBuilder() {
  delete data_;
}

KuduClientBuilder& KuduClientBuilder::clear_master_server_addrs() {
  data_->clear_master_server_addrs();
  return *this;
}

KuduClientBuilder& KuduClientBuilder::master_server_addrs(const vector<string>& addrs) {
  for (const string& addr : addrs) {
    data_->add_master_server_addr(addr);
  }
  return *this;
}

KuduClientBuilder& KuduClientBuilder::add_master_server_addr(const string& addr) {
  data_->add_master_server_addr(addr);
  return *this;
}

KuduClientBuilder& KuduClientBuilder::default_admin_operation_timeout(const MonoDelta& timeout) {
  data_->default_admin_operation_timeout(timeout);
  return *this;
}

KuduClientBuilder& KuduClientBuilder::default_rpc_timeout(const MonoDelta& timeout) {
  data_->default_rpc_timeout(timeout);
  return *this;
}

Status KuduClientBuilder::Build(shared_ptr<KuduClient>* client) {
  return data_->Build(client);
}

////////////////////////////////////////////////////////////
// KuduClient
////////////////////////////////////////////////////////////

KuduClient::KuduClient()
  : data_(new shared_ptr<internal::Client>(new internal::Client())) {
}

KuduClient::~KuduClient() {
  delete data_;
}

KuduTableCreator* KuduClient::NewTableCreator() {
  return new KuduTableCreator(this);
}

Status KuduClient::IsCreateTableInProgress(const string& table_name,
                                           bool *create_in_progress) {
  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(default_admin_operation_timeout());
  return data_->get()->IsCreateTableInProgress(table_name, deadline, create_in_progress);
}

Status KuduClient::DeleteTable(const string& table_name) {
  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(default_admin_operation_timeout());
  return data_->get()->DeleteTable(table_name, deadline);
}

KuduTableAlterer* KuduClient::NewTableAlterer(const string& name) {
  return new KuduTableAlterer(this, name);
}

Status KuduClient::IsAlterTableInProgress(const string& table_name,
                                          bool *alter_in_progress) {
  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(default_admin_operation_timeout());
  return data_->get()->IsAlterTableInProgress(table_name, deadline, alter_in_progress);
}

Status KuduClient::GetTableSchema(const string& table_name,
                                  KuduSchema* schema) {
  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(default_admin_operation_timeout());
  string table_id_ignored;
  PartitionSchema partition_schema;
  return data_->get()->GetTableSchema(table_name,
                                      deadline,
                                      schema,
                                      &partition_schema,
                                      &table_id_ignored);
}

Status KuduClient::ListTabletServers(vector<KuduTabletServer*>* tablet_servers) {
  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(default_admin_operation_timeout());
  return data_->get()->ListTabletServers(tablet_servers, deadline);
}

Status KuduClient::ListTables(vector<string>* tables,
                              const string& filter) {
  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(default_admin_operation_timeout());
  return data_->get()->ListTables(tables, filter, deadline);
}

Status KuduClient::TableExists(const string& table_name, bool* exists) {
  std::vector<std::string> tables;
  RETURN_NOT_OK(ListTables(&tables, table_name));
  for (const string& table : tables) {
    if (table == table_name) {
      *exists = true;
      return Status::OK();
    }
  }
  *exists = false;
  return Status::OK();
}

Status KuduClient::OpenTable(const string& table_name,
                             shared_ptr<KuduTable>* table) {
  KuduSchema schema;
  string table_id;
  PartitionSchema partition_schema;
  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(default_admin_operation_timeout());
  RETURN_NOT_OK(data_->get()->GetTableSchema(table_name,
                                             deadline,
                                             &schema,
                                             &partition_schema,
                                             &table_id));

  // In the future, probably will look up the table in some map to reuse KuduTable
  // instances.
  shared_ptr<KuduTable> ret(new KuduTable(shared_from_this(), table_name, table_id,
                                          schema, partition_schema));
  RETURN_NOT_OK(ret->data_->get()->Open());
  table->swap(ret);

  return Status::OK();
}

shared_ptr<KuduSession> KuduClient::NewSession() {
  shared_ptr<KuduSession> ret(new KuduSession(shared_from_this()));
  (*ret->data_)->Init();
  return ret;
}

bool KuduClient::IsMultiMaster() const {
  return data_->get()->IsMultiMaster();
}

const MonoDelta& KuduClient::default_admin_operation_timeout() const {
  return data_->get()->default_admin_operation_timeout();
}

const MonoDelta& KuduClient::default_rpc_timeout() const {
  return data_->get()->default_rpc_timeout();
}

const uint64_t KuduClient::kNoTimestamp = 0;

uint64_t KuduClient::GetLatestObservedTimestamp() const {
  return data_->get()->GetLatestObservedTimestamp();
}

void KuduClient::SetLatestObservedTimestamp(uint64_t ht_timestamp) {
  return data_->get()->UpdateLatestObservedTimestamp(ht_timestamp);
}

////////////////////////////////////////////////////////////
// KuduTableCreator
////////////////////////////////////////////////////////////

KuduTableCreator::KuduTableCreator(KuduClient* client)
  : data_(new internal::TableCreator(client->data_->get())) {
}

KuduTableCreator::~KuduTableCreator() {
  delete data_;
}

KuduTableCreator& KuduTableCreator::table_name(const string& name) {
  data_->table_name_ = name;
  return *this;
}

KuduTableCreator& KuduTableCreator::schema(const KuduSchema* schema) {
  data_->schema_ = schema;
  return *this;
}

KuduTableCreator& KuduTableCreator::add_hash_partitions(const std::vector<std::string>& columns,
                                                        int32_t num_buckets) {
  return add_hash_partitions(columns, num_buckets, 0);
}

KuduTableCreator& KuduTableCreator::add_hash_partitions(const std::vector<std::string>& columns,
                                                        int32_t num_buckets, int32_t seed) {
  data_->add_hash_partitions(columns, num_buckets, seed);
  return *this;
}

KuduTableCreator& KuduTableCreator::set_range_partition_columns(
    const std::vector<std::string>& columns) {
  data_->set_range_partition_columns(columns);
  return *this;
}

KuduTableCreator& KuduTableCreator::split_rows(const vector<const KuduPartialRow*>& rows) {
  data_->split_rows_ = rows;
  return *this;
}

KuduTableCreator& KuduTableCreator::add_split_row(const KuduPartialRow* split_row) {
  data_->split_rows_.push_back(split_row);
  return *this;
}

KuduTableCreator& KuduTableCreator::num_replicas(int num_replicas) {
  data_->num_replicas_ = num_replicas;
  return *this;
}

KuduTableCreator& KuduTableCreator::timeout(const MonoDelta& timeout) {
  data_->timeout_ = timeout;
  return *this;
}

KuduTableCreator& KuduTableCreator::wait(bool wait) {
  data_->wait_ = wait;
  return *this;
}

Status KuduTableCreator::Create() {
  return data_->Create();
}

////////////////////////////////////////////////////////////
// KuduTable
////////////////////////////////////////////////////////////

KuduTable::KuduTable(const shared_ptr<KuduClient>& client,
                     const string& name,
                     const string& table_id,
                     const KuduSchema& schema,
                     const PartitionSchema& partition_schema)
  : data_(new shared_ptr<internal::Table>(new internal::Table(*client->data_, name, table_id, schema, partition_schema))) {
}

KuduTable::~KuduTable() {
  delete data_;
}

const string& KuduTable::name() const {
  return data_->get()->name_;
}

const string& KuduTable::id() const {
  return data_->get()->id_;
}

const KuduSchema& KuduTable::schema() const {
  return data_->get()->schema_;
}

KuduInsert* KuduTable::NewInsert() {
  return new KuduInsert(shared_from_this());
}

KuduUpdate* KuduTable::NewUpdate() {
  return new KuduUpdate(shared_from_this());
}

KuduDelete* KuduTable::NewDelete() {
  return new KuduDelete(shared_from_this());
}

KuduClient* KuduTable::client() const {
  // TODO: add back KuduClient::Data with an internal KuduClient
  return nullptr;
}

const PartitionSchema& KuduTable::partition_schema() const {
  return data_->get()->partition_schema_;
}

KuduPredicate* KuduTable::NewComparisonPredicate(const Slice& col_name,
                                                 KuduPredicate::ComparisonOp op,
                                                 KuduValue* value) {
  StringPiece name_sp(reinterpret_cast<const char*>(col_name.data()), col_name.size());
  const Schema* s = data_->get()->schema_.schema_;
  int col_idx = s->find_column(name_sp);
  if (col_idx == Schema::kColumnNotFound) {
    // Since this function doesn't return an error, instead we create a special
    // predicate that just returns the errors when we add it to the scanner.
    //
    // This makes the API more "fluent".
    delete value; // we always take ownership of 'value'.
    return new KuduPredicate(new ErrorPredicateData(
                                 Status::NotFound("column not found", col_name)));
  }

  return new KuduPredicate(new ComparisonPredicateData(s->column(col_idx), op, value));
}

////////////////////////////////////////////////////////////
// Error
////////////////////////////////////////////////////////////

const Status& KuduError::status() const {
  return data_->status_;
}

const KuduWriteOperation& KuduError::failed_op() const {
  return *data_->failed_op_;
}

KuduWriteOperation* KuduError::release_failed_op() {
  CHECK_NOTNULL(data_->failed_op_.get());
  return data_->failed_op_.release();
}

bool KuduError::was_possibly_successful() const {
  // TODO: implement me - right now be conservative.
  return true;
}

KuduError::KuduError(KuduWriteOperation* failed_op,
                     const Status& status)
  : data_(new KuduError::Data(gscoped_ptr<KuduWriteOperation>(failed_op),
                              status)) {
}

KuduError::~KuduError() {
  delete data_;
}

////////////////////////////////////////////////////////////
// KuduSession
////////////////////////////////////////////////////////////

KuduSession::KuduSession(const shared_ptr<KuduClient>& client)
  : data_(new std::shared_ptr<internal::Session>(new internal::Session(*client->data_))) {
}

KuduSession::~KuduSession() {
  WARN_NOT_OK(data_->get()->Close(true), "Closed Session with pending operations.");
  delete data_;
}

Status KuduSession::Close() {
  return data_->get()->Close(false);
}

Status KuduSession::SetFlushMode(FlushMode m) {
  if (!tight_enum_test<FlushMode>(m)) {
    // Be paranoid in client code.
    return Status::InvalidArgument("Bad flush mode");
  }
  return data_->get()->SetFlushMode(m);
}

Status KuduSession::SetExternalConsistencyMode(ExternalConsistencyMode m) {
  if (!tight_enum_test<ExternalConsistencyMode>(m)) {
    // Be paranoid in client code.
    return Status::InvalidArgument("Bad external consistency mode");
  }
  return data_->get()->SetExternalConsistencyMode(m);
}

void KuduSession::SetTimeoutMillis(int millis) {
  return data_->get()->SetTimeoutMillis(millis);
}

Status KuduSession::Flush() {
  return data_->get()->Flush();
}

void KuduSession::FlushAsync(KuduStatusCallback* user_callback) {
  return data_->get()->FlushAsync(user_callback);
}

bool KuduSession::HasPendingOperations() const {
  return data_->get()->HasPendingOperations();
}

Status KuduSession::Apply(KuduWriteOperation* write_op) {
  return data_->get()->Apply(write_op);
}

int KuduSession::CountBufferedOperations() const {
  return data_->get()->CountBufferedOperations();
}

int KuduSession::CountPendingErrors() const {
  return data_->get()->error_collector_->CountErrors();
}

void KuduSession::GetPendingErrors(vector<KuduError*>* errors, bool* overflowed) {
  data_->get()->error_collector_->GetErrors(errors, overflowed);
}

KuduClient* KuduSession::client() const {
  // TODO: add back KuduSession::Data with an internal KuduClient
  return nullptr;
}

////////////////////////////////////////////////////////////
// KuduTableAlterer
////////////////////////////////////////////////////////////
KuduTableAlterer::KuduTableAlterer(KuduClient* client, const string& name)
  : data_(new Data(client, name)) {
}

KuduTableAlterer::~KuduTableAlterer() {
  delete data_;
}

KuduTableAlterer* KuduTableAlterer::RenameTo(const string& new_name) {
  data_->RenameTo(new_name);
  return this;
}

KuduColumnSpec* KuduTableAlterer::AddColumn(const string& name) {
  return data_->AddColumn(name);
}

KuduColumnSpec* KuduTableAlterer::AlterColumn(const string& name) {
  return data_->AlterColumn(name);
}

KuduTableAlterer* KuduTableAlterer::DropColumn(const string& name) {
  data_->DropColumn(name);
  return this;
}

KuduTableAlterer* KuduTableAlterer::timeout(const MonoDelta& timeout) {
  data_->timeout(timeout);
  return this;
}

KuduTableAlterer* KuduTableAlterer::wait(bool wait) {
  data_->wait(wait);
  return this;
}

Status KuduTableAlterer::Alter() {
  return data_->Alter();
}

////////////////////////////////////////////////////////////
// KuduScanner
////////////////////////////////////////////////////////////

KuduScanner::KuduScanner(KuduTable* table)
  : data_(new KuduScanner::Data(table)) {
}

KuduScanner::~KuduScanner() {
  Close();
  delete data_;
}

Status KuduScanner::SetProjectedColumns(const vector<string>& col_names) {
  return SetProjectedColumnNames(col_names);
}

Status KuduScanner::SetProjectedColumnNames(const vector<string>& col_names) {
  if (data_->open_) {
    return Status::IllegalState("Projection must be set before Open()");
  }
  return data_->mutable_configuration()->SetProjectedColumnNames(col_names);
}

Status KuduScanner::SetProjectedColumnIndexes(const vector<int>& col_indexes) {
  if (data_->open_) {
    return Status::IllegalState("Projection must be set before Open()");
  }
  return data_->mutable_configuration()->SetProjectedColumnIndexes(col_indexes);
}

Status KuduScanner::SetBatchSizeBytes(uint32_t batch_size) {
  return data_->mutable_configuration()->SetBatchSizeBytes(batch_size);
}

Status KuduScanner::SetReadMode(ReadMode read_mode) {
  if (data_->open_) {
    return Status::IllegalState("Read mode must be set before Open()");
  }
  if (!tight_enum_test<ReadMode>(read_mode)) {
    return Status::InvalidArgument("Bad read mode");
  }
  return data_->mutable_configuration()->SetReadMode(read_mode);
}

Status KuduScanner::SetOrderMode(OrderMode order_mode) {
  if (data_->open_) {
    return Status::IllegalState("Order mode must be set before Open()");
  }
  if (!tight_enum_test<OrderMode>(order_mode)) {
    return Status::InvalidArgument("Bad order mode");
  }
  return data_->mutable_configuration()->SetFaultTolerant(order_mode == ORDERED);
}

Status KuduScanner::SetFaultTolerant() {
  if (data_->open_) {
    return Status::IllegalState("Fault-tolerance must be set before Open()");
  }
  return data_->mutable_configuration()->SetFaultTolerant(true);
}

Status KuduScanner::SetSnapshotMicros(uint64_t snapshot_timestamp_micros) {
  if (data_->open_) {
    return Status::IllegalState("Snapshot timestamp must be set before Open()");
  }
  data_->mutable_configuration()->SetSnapshotMicros(snapshot_timestamp_micros);
  return Status::OK();
}

Status KuduScanner::SetSnapshotRaw(uint64_t snapshot_timestamp) {
  if (data_->open_) {
    return Status::IllegalState("Snapshot timestamp must be set before Open()");
  }
  data_->mutable_configuration()->SetSnapshotRaw(snapshot_timestamp);
  return Status::OK();
}

Status KuduScanner::SetSelection(KuduClient::ReplicaSelection selection) {
  if (data_->open_) {
    return Status::IllegalState("Replica selection must be set before Open()");
  }
  return data_->mutable_configuration()->SetSelection(selection);
}

Status KuduScanner::SetTimeoutMillis(int millis) {
  if (data_->open_) {
    return Status::IllegalState("Timeout must be set before Open()");
  }
  data_->mutable_configuration()->SetTimeoutMillis(millis);
  return Status::OK();
}

Status KuduScanner::AddConjunctPredicate(KuduPredicate* pred) {
  if (data_->open_) {
    // Take ownership even if we return a bad status.
    delete pred;
    return Status::IllegalState("Predicate must be set before Open()");
  }
  return data_->mutable_configuration()->AddConjunctPredicate(pred);
}

Status KuduScanner::AddLowerBound(const KuduPartialRow& key) {
  return data_->mutable_configuration()->AddLowerBound(key);
}

Status KuduScanner::AddLowerBoundRaw(const Slice& key) {
  return data_->mutable_configuration()->AddLowerBoundRaw(key);
}

Status KuduScanner::AddExclusiveUpperBound(const KuduPartialRow& key) {
  return data_->mutable_configuration()->AddUpperBound(key);
}

Status KuduScanner::AddExclusiveUpperBoundRaw(const Slice& key) {
  return data_->mutable_configuration()->AddUpperBoundRaw(key);
}

Status KuduScanner::AddLowerBoundPartitionKeyRaw(const Slice& partition_key) {
  return data_->mutable_configuration()->AddLowerBoundPartitionKeyRaw(partition_key);
}

Status KuduScanner::AddExclusiveUpperBoundPartitionKeyRaw(const Slice& partition_key) {
  return data_->mutable_configuration()->AddUpperBoundPartitionKeyRaw(partition_key);
}

Status KuduScanner::SetCacheBlocks(bool cache_blocks) {
  if (data_->open_) {
    return Status::IllegalState("Block caching must be set before Open()");
  }
  return data_->mutable_configuration()->SetCacheBlocks(cache_blocks);
}

KuduSchema KuduScanner::GetProjectionSchema() const {
  return KuduSchema(*data_->configuration().projection());
}

string KuduScanner::ToString() const {
  return data_->ToString();
}

Status KuduScanner::Open() {
  return data_->Open();
}

Status KuduScanner::KeepAlive() {
  return data_->KeepAlive();
}

void KuduScanner::Close() {
  data_->Close();
}

bool KuduScanner::HasMoreRows() const {
  return data_->HasMoreRows();
}

Status KuduScanner::NextBatch(vector<KuduRowResult>* rows) {
  RETURN_NOT_OK(NextBatch(&data_->batch_for_old_api_));
  data_->batch_for_old_api_.data_->ExtractRows(rows);
  return Status::OK();
}

Status KuduScanner::NextBatch(KuduScanBatch* batch) {
  return data_->NextBatch(batch);
}

Status KuduScanner::GetCurrentServer(KuduTabletServer** server) {
  return data_->GetCurrentServer(server);
}

////////////////////////////////////////////////////////////
// KuduScanToken
////////////////////////////////////////////////////////////

KuduScanToken::KuduScanToken(KuduScanToken::Data* data)
    : data_(data) {
}

KuduScanToken::~KuduScanToken() {
  delete data_;
}

Status KuduScanToken::IntoKuduScanner(KuduScanner** scanner) const {
  return data_->IntoKuduScanner(scanner);
}

const vector<KuduTabletServer*>& KuduScanToken::TabletServers() const {
  return data_->TabletServers();
}

Status KuduScanToken::Serialize(string* buf) const {
  return data_->Serialize(buf);
}

Status KuduScanToken::DeserializeIntoScanner(KuduClient* client,
                                         const string& serialized_token,
                                         KuduScanner** scanner) {
  return KuduScanToken::Data::DeserializeIntoScanner(client, serialized_token, scanner);
}

////////////////////////////////////////////////////////////
// KuduScanTokenBuilder
////////////////////////////////////////////////////////////

KuduScanTokenBuilder::KuduScanTokenBuilder(KuduTable* table)
    : data_(new KuduScanTokenBuilder::Data(table)) {
}

KuduScanTokenBuilder::~KuduScanTokenBuilder() {
  delete data_;
}

Status KuduScanTokenBuilder::SetProjectedColumnNames(const vector<string>& col_names) {
  return data_->mutable_configuration()->SetProjectedColumnNames(col_names);
}

Status KuduScanTokenBuilder::SetProjectedColumnIndexes(const vector<int>& col_indexes) {
  return data_->mutable_configuration()->SetProjectedColumnIndexes(col_indexes);
}

Status KuduScanTokenBuilder::SetBatchSizeBytes(uint32_t batch_size) {
  return data_->mutable_configuration()->SetBatchSizeBytes(batch_size);
}

Status KuduScanTokenBuilder::SetReadMode(KuduScanner::ReadMode read_mode) {
  if (!tight_enum_test<KuduScanner::ReadMode>(read_mode)) {
    return Status::InvalidArgument("Bad read mode");
  }
  return data_->mutable_configuration()->SetReadMode(read_mode);
}

Status KuduScanTokenBuilder::SetFaultTolerant() {
  return data_->mutable_configuration()->SetFaultTolerant(true);
}

Status KuduScanTokenBuilder::SetSnapshotMicros(uint64_t snapshot_timestamp_micros) {
  data_->mutable_configuration()->SetSnapshotMicros(snapshot_timestamp_micros);
  return Status::OK();
}

Status KuduScanTokenBuilder::SetSnapshotRaw(uint64_t snapshot_timestamp) {
  data_->mutable_configuration()->SetSnapshotRaw(snapshot_timestamp);
  return Status::OK();
}

Status KuduScanTokenBuilder::SetSelection(KuduClient::ReplicaSelection selection) {
  return data_->mutable_configuration()->SetSelection(selection);
}

Status KuduScanTokenBuilder::SetTimeoutMillis(int millis) {
  data_->mutable_configuration()->SetTimeoutMillis(millis);
  return Status::OK();
}

Status KuduScanTokenBuilder::AddConjunctPredicate(KuduPredicate* pred) {
  return data_->mutable_configuration()->AddConjunctPredicate(pred);
}

Status KuduScanTokenBuilder::AddLowerBound(const KuduPartialRow& key) {
  return data_->mutable_configuration()->AddLowerBound(key);
}

Status KuduScanTokenBuilder::AddUpperBound(const KuduPartialRow& key) {
  return data_->mutable_configuration()->AddUpperBound(key);
}

Status KuduScanTokenBuilder::SetCacheBlocks(bool cache_blocks) {
  return data_->mutable_configuration()->SetCacheBlocks(cache_blocks);
}

Status KuduScanTokenBuilder::Build(vector<KuduScanToken*>* tokens) {
  return data_->Build(tokens);
}

////////////////////////////////////////////////////////////
// KuduTabletServer
////////////////////////////////////////////////////////////

KuduTabletServer::KuduTabletServer()
  : data_(nullptr) {
}

KuduTabletServer::~KuduTabletServer() {
  delete data_;
}

const string& KuduTabletServer::uuid() const {
  return data_->uuid_;
}

const string& KuduTabletServer::hostname() const {
  return data_->hostname_;
}

} // namespace client
} // namespace kudu
