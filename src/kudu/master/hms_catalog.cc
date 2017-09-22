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
#include "kudu/hms/hive_metastore_constants.h"
#include "kudu/hms/hms_client.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/sys_catalog.h"
#include "kudu/util/async_util.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/net/net_util.h"

using boost::optional;
using rapidjson::Document;
using rapidjson::Value;
using std::deque;
using std::string;
using std::vector;
using strings::CharSet;

namespace kudu {
namespace master {

HmsCatalog::HmsCatalog(vector<HostPort> addresses)
    : addresses_(std::move(addresses)),
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
  int consecutive_errors = 0;
  while (running_) {

    // Wait for RPCs to be ready to send.
    {
      MutexLock lock(lock_);
      if (rpc_queue_.empty()) {
        cond_.Wait();
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
        if (!rpc_queue_.empty()) {
          // More RPCs have been queued since we reset the queue. Immediately
          // reenter the loop in order to service them.
          continue;
        }

        // Backoff so that we don't attempt to connect to the HMS in a hot loop
        // when it's unreachable. We'll automatically be woken up if an RPC is
        // enqueued, so we can afford long backoffs.
        int wait = 1000 * std::min(consecutive_errors, 60);
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
  }

  Reset(Status::ServiceUnavailable("HMS Catalog shutting down"));
}

} // namespace master
} // namespace kudu
