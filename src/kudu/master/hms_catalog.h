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

#include <atomic>
#include <deque>
#include <functional>
#include <string>
#include <vector>

#include <boost/optional/optional.hpp>

#include "kudu/common/schema.h"
#include "kudu/hms/hms_client.h"
#include "kudu/util/condition_variable.h"
#include "kudu/util/mutex.h"
#include "kudu/util/status.h"

namespace kudu {

class HostPort;

namespace master {

class CatalogManager;

struct Rpc {
  // The run function, which takes an HMS client and returns a Status.
  std::function<Status(hms::HmsClient*)> run;

  // The callback function, which takes the result of the run function, or a
  // connection error if the HMS is unreachable.
  std::function<void(Status)> callback;
};

class HmsCatalog {
 public:

  HmsCatalog(std::vector<HostPort> addresses, CatalogManager* catalog_manager);
  ~HmsCatalog();

  Status Start();
  void Stop();

  Status CreateTable(const std::string& id,
                     const std::string& name,
                     const Schema& schema) WARN_UNUSED_RESULT;

  Status DropTable(const std::string& id,
                   const std::string& name) WARN_UNUSED_RESULT;

  Status AlterTable(const std::string& id,
                    const std::string& name,
                    const std::string& new_name,
                    const Schema& schema) WARN_UNUSED_RESULT;

 private:

  Status CreateTableObject(const std::string& id,
                           const std::string& name,
                           const Schema& schema,
                           hive::Table* table) WARN_UNUSED_RESULT;

  Status ParseTableName(const std::string& table,
                        std::string* hms_database,
                        std::string* hms_table) WARN_UNUSED_RESULT;

  Status Reconnect();
  void Reset(const Status& s);

  void Run();

  Status HandleAlterTableEvent(const hive::NotificationEvent& event) WARN_UNUSED_RESULT;
  Status HandleDropTableEvent(const hive::NotificationEvent& event) WARN_UNUSED_RESULT;

  const std::vector<HostPort> addresses_;
  CatalogManager* const catalog_manager_;

  // Only mutated by the worker thread.
  boost::optional<hms::HmsClient> client_;

  mutable Mutex lock_;
  ConditionVariable cond_;

  // Signals the worker to shutdown. Read by the worker thread without the lock
  // taken, but mutated externally with the lock held.
  std::atomic<bool> running_;

  std::deque<Rpc> rpc_queue_;
  scoped_refptr<Thread> worker_;
};

} // namespace master
} // namespace kudu
