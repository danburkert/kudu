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

#include "kudu/master/hms_catalog.h"

#include <algorithm>
#include <iostream>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <rapidjson/document.h>

#include "kudu/gutil/strings/charset.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/hms/hive_metastore_constants.h"
#include "kudu/hms/hms_client.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/sys_catalog.h"
#include "kudu/util/async_util.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/net/net_util.h"

DEFINE_int32(hms_poll_period_ms, 1000,
             "Amount of time the HMS catalog background task thread waits "
             "between polling the Hive Metastore for catalog updates.");
TAG_FLAG(hms_poll_period_ms, evolving);
TAG_FLAG(hms_poll_period_ms, advanced);

using boost::optional;
using rapidjson::Document;
using rapidjson::Value;
using std::deque;
using std::string;
using std::vector;
using strings::CharSet;
using strings::Substitute;

namespace kudu {
namespace master {

HmsCatalog::HmsCatalog(vector<HostPort> addresses, CatalogManager* catalog_manager)
    : addresses_(std::move(addresses)),
      catalog_manager_(catalog_manager),
      cond_(&lock_),
      running_(false) {
  CHECK(!addresses_.empty());
}

HmsCatalog::~HmsCatalog() {
  Stop();
}

Status HmsCatalog::Start() {
  MutexLock lock(lock_);
  if (running_) {
    return Status::IllegalState("HMS Catalog is already started");
  }

  running_ = true;
  return Thread::Create("catalog manager", "hms", &HmsCatalog::Run, this, &worker_);
}

void HmsCatalog::Stop() {
  Thread* worker;
  {
    MutexLock lock(lock_);
    worker = worker_.get();

    if (running_) {
      running_ = false;
      cond_.Broadcast();
    }
  }

  if (worker != nullptr) {
    WARN_NOT_OK(ThreadJoiner(worker).Join(), "failed to stop HMS catalog task");
  }
}

Status HmsCatalog::CreateTable(const string& id,
                               const string& name,
                               const Schema& schema) {
  hive::Table table;
  RETURN_NOT_OK(CreateTableObject(id, name, schema, &table));

  Rpc rpc;
  rpc.run = [=] (hms::HmsClient* client) {
    return client->CreateTable(table);
  };

  Synchronizer s;
  rpc.callback = s.AsStdStatusCallback();

  {
    MutexLock lock(lock_);
    CHECK(running_);
    rpc_queue_.emplace_back(std::move(rpc));
    if (rpc_queue_.size() == 1) {
      cond_.Signal();
    }
  }

  return s.Wait();
}

Status HmsCatalog::DropTable(const string& id, const string& name) {
  string hms_database;
  string hms_table;
  RETURN_NOT_OK(ParseTableName(name, &hms_database, &hms_table));

  hive::EnvironmentContext env_ctx;
  env_ctx.__set_properties({ make_pair(hms::HmsClient::kKuduTableIdKey, id) });

  Rpc rpc;
  rpc.run = [=] (hms::HmsClient* client) {
    return client->DropTableWithContext(hms_database, hms_table, env_ctx);
  };

  Synchronizer s;
  rpc.callback = s.AsStdStatusCallback();

  {
    MutexLock lock(lock_);
    rpc_queue_.emplace_back(std::move(rpc));
    if (rpc_queue_.size() == 1) {
      cond_.Signal();
    }
  }

  return s.Wait();
}

Status HmsCatalog::AlterTable(const string& id,
                              const string& name,
                              const string& new_name,
                              const Schema& schema) {
  hive::Table table;
  RETURN_NOT_OK(CreateTableObject(id, new_name, schema, &table));

  string hms_database;
  string hms_table;
  if (!ParseTableName(name, &hms_database, &hms_table).ok()) {
    // Parsing the original table name has failed, so it can not be present in
    // the HMS. Instead of altering the table, create it in the HMS as a new table.
    return CreateTable(id, new_name, schema);
  }

  Rpc rpc;
  rpc.run = [=] (hms::HmsClient* client) {
    // TODO(dan): recover from NotFound and create the table.
    return client->AlterTable(hms_database, hms_table, table);
  };

  Synchronizer s;
  rpc.callback = s.AsStdStatusCallback();

  {
    MutexLock lock(lock_);
    CHECK(running_);
    rpc_queue_.emplace_back(std::move(rpc));
    if (rpc_queue_.size() == 1) {
      cond_.Signal();
    }
  }

  return s.Wait();
}

Status HmsCatalog::CreateTableObject(const string& id,
                                     const string& name,
                                     const Schema& schema,
                                     hive::Table* table) {
  RETURN_NOT_OK(ParseTableName(name, &table->dbName, &table->tableName));
  table->tableType = "MANAGED_TABLE";

  table->parameters = {
    make_pair(hms::HmsClient::kKuduTableIdKey, id),
    make_pair(hms::HmsClient::kKuduMasterAddrsKey, string("TODO")),
    make_pair(hive::g_hive_metastore_constants.META_TABLE_STORAGE,
              hms::HmsClient::kKuduStorageHandler),
  };
  return Status::OK();
}

Status HmsCatalog::ParseTableName(const string& table,
                                  string* hms_database,
                                  string* hms_table) {
  CharSet charset("abcdefghijklmnopqrstuvwxyz_");
  const char kSeparator = '.';

  const char* kErrorMessage =
            "Kudu table name must contain only lower-case ASCII characters, "
            "underscores ('_') characters, and a database/table separator ('.') "
            "when Hive Metastore integration is enabled";

  optional<int> separator_idx;
  for (int idx = 0; idx < table.size(); idx++) {
    char c = table[idx];
    if (!charset.Test(c)) {
      if (c == kSeparator) {
        if (separator_idx) {
          return Status::InvalidArgument(kErrorMessage, table);
        }
        separator_idx = idx;
      } else {
        return Status::InvalidArgument(kErrorMessage, table);
      }
    }
  }

  if (!separator_idx || *separator_idx == 0 || *separator_idx == table.size() - 1) {
    return Status::InvalidArgument(kErrorMessage, table);
  }

  *hms_database = string(table, 0, *separator_idx);
  *hms_table = string(table, *separator_idx + 1);
  return Status::OK();
}

Status HmsCatalog::Reconnect() {
  Status s;
  for (const HostPort& address : addresses_) {
    client_ = hms::HmsClient(address);

    s = client_->Start();

    if (s.ok()) {
      return Status::OK();
    }

    LOG(WARNING) << "Unable to connect to the Hive Metastore ("
                 << address.ToString() << "): " << s.ToString();
  }

  client_ = boost::none;
  return s;
}

void HmsCatalog::Reset(const Status& s) {
  deque<Rpc> rpcs;
  {
      MutexLock lock(lock_);
      rpc_queue_.swap(rpcs);
  }

  for (auto& rpc : rpcs) {
    rpc.callback(s);
  }
}

void HmsCatalog::Run() {

  int64_t last_event_id;
  {
    Status s = catalog_manager_->sys_catalog()->GetNotificationLogEventId(&last_event_id);
    WARN_NOT_OK(s, "failed to get current notification log event ID");
    if (!s.ok()) {
      Reset(s);
    }
  }

  // Set the initial poll time in the past so that the initial turn of the loop
  // below does not wait.
  MonoTime last_poll = MonoTime::Min();

  int consecutive_errors = 0;
outer_loop:
  while (running_) {

    // If there are no queued RPCs, and we are not ready to poll the
    // HMS again, wait until the next poll time.
    MonoTime now = MonoTime::Now();
    MonoTime next_poll = last_poll + MonoDelta::FromMilliseconds(FLAGS_hms_poll_period_ms);
    if (now.ComesBefore(next_poll)) {
      MutexLock lock(lock_);
      if (rpc_queue_.empty()) {
        cond_.TimedWait(next_poll - now);
      }
    }

    // If the client is not already connected, reconnect it.
    if (!client_) {
      Status s = Reconnect();

      if (!s.ok()) {
        consecutive_errors++;
        // Connecting to the HMS failed. Fail all queued RPCs.
        Reset(s);

        MutexLock lock(lock_);
        if (rpc_queue_.empty()) {
          // More RPCs have been queued since we reset the queue. Immediately
          // reenter the loop in order to service them.
          continue;
        }

        // Backoff so that we don't attempt to connect to the HMS with every
        // single poll period when it's unreachable. We'll automatically be
        // woken up if an RPC is enqueued, so we can afford long backoffs.
        int wait = FLAGS_hms_poll_period_ms * std::min(consecutive_errors, 60);
        cond_.TimedWait(MonoDelta::FromMilliseconds(wait));
      }
    }
    consecutive_errors = 0;

    // Pick RPCs off the queue and complete them.
    while (true) {
      Rpc rpc;
      {
        MutexLock lock(lock_);
        if (rpc_queue_.empty()) break;
        rpc = std::move(rpc_queue_.front());
        rpc_queue_.pop_front();
      }

      Status s = rpc.run(&client_.get());
      rpc.callback(s);
      // If it's an IO error, assume the connection is toast.
      if (s.IsIOError()) {
        client_ = boost::none;
        break;
      }
    }

    if (next_poll.ComesBefore(MonoTime::Now())) {
      last_poll = MonoTime::Now();

      CatalogManager::ScopedLeaderSharedLock l(catalog_manager_);
      if (!l.first_failed_status().ok()) {
        VLOG(1) << "Catalog manager not leader; skipping notification log retrieval: "
                << l.first_failed_status().ToString();
        continue;
      }

      const int kEventBatchSize = 100;
      vector<hive::NotificationEvent> events;
      while (true) {
        events.clear();
        Status s = client_->GetNotificationEvents(last_event_id, kEventBatchSize, &events);

        WARN_NOT_OK(s, "failed to retrieve notification log events");
        if (!s.ok()) {
          // If it's an IO error, assume the connection is toast.
          if (s.IsIOError()) client_ = boost::none;
          LOG(WARNING) << "Failed to retrieve notification log events: " << s.ToString();
          continue;
        }

        for (const auto& event : events) {
          VLOG(2) << "Received notification log event: " << event;

          if (event.eventId <= last_event_id) {
            LOG(DFATAL) << "Received notification log event with invalid event ID: " << event;
            continue;
          }

          Status s;
          if (event.eventType == "ALTER_TABLE") {
            s = HandleAlterTableEvent(event);
          } else if (event.eventType == "DROP_TABLE") {
            s = HandleDropTableEvent(event);
          }

          if (!s.ok()) {
            if (l.has_term_changed()) {
              LOG(INFO) << "Term changed while processing notification log; "
                           "skipping remaining entries: "
                        << s.ToString();
              goto outer_loop;
            }

            string table_name = Substitute("$0.$1", event.dbName, event.tableName);
            if (s.IsNotFound()) {
              // This happens for every rename/drop table event that originates
              // in Kudu, so it's too noisy to log at INFO level.
              VLOG(1) << "Ignoring " << event.eventType
                      << " notification log event for unknown table: " << table_name;
            } else {
              LOG(DFATAL) << "Failed to handle " << event.eventType
                          << " event for " << table_name << ": " << s.ToString();
            }
          }

          last_event_id = event.eventId;
        }

        // If the last set of events was smaller than the batch size then we can
        // assume that we've read all of the available events.
        if (events.size() < kEventBatchSize) break;
      }
    }
  }

  Reset(Status::ServiceUnavailable("HMS Catalog shutting down"));
}

Status HmsCatalog::HandleAlterTableEvent(const hive::NotificationEvent& event) {
  Document document;

  const char* kBeforeTableJsonKey = "tableObjBeforeJson";
  const char* kAfterTableJsonKey = "tableObjAfterJson";

  // Note: if the table name is being altered, this will be the new table name.
  string table_name = Substitute("$0.$1", event.dbName, event.tableName);

  Document doc;

  if (event.messageFormat != "json-0.2" ||
      document.Parse<0>(event.message.c_str()).HasParseError() ||
      !document.HasMember(kBeforeTableJsonKey) ||
      !document[kBeforeTableJsonKey].IsString() ||
      !document.HasMember(kAfterTableJsonKey) ||
      !document[kAfterTableJsonKey].IsString()) {
    return Status::Corruption("invalid Hive Metastore event", table_name);
  }

  hive::Table before_table;
  const Value& beforeTableObjJson = document[kBeforeTableJsonKey];
  RETURN_NOT_OK(hms::HmsClient::DeserializeJsonTable(Slice(beforeTableObjJson.GetString(),
                                                           beforeTableObjJson.GetStringLength()),
                                                     &before_table));

  const string* storage_handler =
      FindOrNull(before_table.parameters, hive::g_hive_metastore_constants.META_TABLE_STORAGE);
  if (!storage_handler || *storage_handler != hms::HmsClient::kKuduStorageHandler) {
    // Not a Kudu table; skip it.
    return Status::OK();
  }

  hive::Table after_table;
  const Value& afterTableObjJson = document[kAfterTableJsonKey];
  RETURN_NOT_OK(hms::HmsClient::DeserializeJsonTable(Slice(afterTableObjJson.GetString(),
                                                           afterTableObjJson.GetStringLength()),
                                                     &after_table));

  // Double check that the Kudu HMS plugin is enforcing storage handler and
  // table ID constraints correctly.
  const string* after_storage_handler =
      FindOrNull(before_table.parameters, hive::g_hive_metastore_constants.META_TABLE_STORAGE);
  if (!after_storage_handler || *after_storage_handler != *storage_handler) {
    return Status::IllegalState("Hive Metastore storage handler property altered", table_name);
  }
  const string* table_id = FindOrNull(before_table.parameters, hms::HmsClient::kKuduTableIdKey);
  if (!table_id) {
    return Status::Corruption("invalid Hive Metastore table: missing Kudu table ID", table_name);
  }
  const string* after_table_id = FindOrNull(after_table.parameters,
                                            hms::HmsClient::kKuduTableIdKey);
  if (!after_table_id || *after_table_id != *table_id) {
    return Status::Corruption("Hive Metastore table ID property altered", table_name);
  }

  // Check if the table has been renamed, and if so, attempt to rename it in Kudu.
  if (before_table.dbName != after_table.dbName ||
      before_table.tableName != after_table.tableName) {
    string before_table_name = Substitute("$0.$1", before_table.dbName, before_table.tableName);
    string after_table_name = Substitute("$0.$1", after_table.dbName, after_table.tableName);

    AlterTableRequestPB req;
    AlterTableResponsePB resp;
    req.mutable_table()->set_table_id(*table_id);
    req.mutable_table()->set_table_name(before_table_name);
    *req.mutable_new_table_name() = after_table_name;

    return catalog_manager_->AlterTable(&req, &resp, nullptr, event.eventId);
  }

  return Status::OK();
}

Status HmsCatalog::HandleDropTableEvent(const hive::NotificationEvent& event) {
  Document document;
  const char* kTableJsonKey = "tableObjJson";

  string table_name = Substitute("$0.$1", event.dbName, event.tableName);

  Document doc;
  CHECK_EQ(event.messageFormat, "json-0.2");
  CHECK(!doc.Parse<0>(event.message.c_str()).HasParseError());
  CHECK(doc.HasMember(kTableJsonKey)) << event;
  CHECK(doc[kTableJsonKey].IsString());

  if (event.messageFormat != "json-0.2" ||
      document.Parse<0>(event.message.c_str()).HasParseError() ||
      !document.HasMember(kTableJsonKey) ||
      !document[kTableJsonKey].IsString()) {
    return Status::Corruption("invalid Hive Metastore event", table_name);
  }

  hive::Table table;
  const Value& tableObjJson = document[kTableJsonKey];
  RETURN_NOT_OK(hms::HmsClient::DeserializeJsonTable(Slice(tableObjJson.GetString(),
                                                           tableObjJson.GetStringLength()),
                                                     &table));

  const string* storage_handler =
      FindOrNull(table.parameters, hive::g_hive_metastore_constants.META_TABLE_STORAGE);
  if (!storage_handler || *storage_handler != hms::HmsClient::kKuduStorageHandler) {
    // Not a Kudu table; skip it.
    return Status::OK();
  }

  const string* table_id = FindOrNull(table.parameters, hms::HmsClient::kKuduTableIdKey);
  if (!table_id) {
    return Status::Corruption("invalid Hive Metastore table: missing Kudu table ID", table_name);
  }

  DeleteTableRequestPB req;
  DeleteTableResponsePB resp;
  req.mutable_table()->set_table_id(*table_id);
  req.mutable_table()->set_table_name(table_name);

  return catalog_manager_->DeleteTable(&req, &resp, nullptr, event.eventId);
}

} // namespace master
} // namespace kudu
