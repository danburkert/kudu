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

#include "kudu/client/client_builder-internal.h"

#include "kudu/client/client-internal.h"
#include "kudu/client/meta_cache.h"
#include "kudu/rpc/messenger.h"
#include "kudu/util/init.h"
#include "kudu/util/net/dns_resolver.h"

namespace kudu {

using rpc::MessengerBuilder;

namespace client {

KuduClientBuilder::Data::Data()
  : default_admin_operation_timeout_(MonoDelta::FromSeconds(10)),
    default_rpc_timeout_(MonoDelta::FromSeconds(5)) {
}

void KuduClientBuilder::Data::clear_master_server_addrs() {
  master_server_addrs_.clear();
}

void KuduClientBuilder::Data::add_master_server_addr(const string& addr) {
  master_server_addrs_.push_back(addr);
}

void KuduClientBuilder::Data::default_admin_operation_timeout(const MonoDelta& timeout) {
  default_admin_operation_timeout_ = timeout;
}

void KuduClientBuilder::Data::default_rpc_timeout(const MonoDelta& timeout) {
  default_rpc_timeout_ = timeout;
}

Status KuduClientBuilder::Data::Build(shared_ptr<KuduClient>* client) {
  RETURN_NOT_OK(CheckCPUFlags());

  shared_ptr<KuduClient> c(new KuduClient());

  // Init messenger.
  MessengerBuilder builder("client");
  RETURN_NOT_OK(builder.Build(&c->data_->get()->messenger_));

  (*c->data_)->master_server_addrs_ = master_server_addrs_;
  (*c->data_)->default_admin_operation_timeout_ = default_admin_operation_timeout_;
  (*c->data_)->default_rpc_timeout_ = default_rpc_timeout_;

  // Let's allow for plenty of time for discovering the master the first
  // time around.
  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(c->default_admin_operation_timeout());
  RETURN_NOT_OK_PREPEND(c->data_->get()->SetMasterServerProxy(deadline),
                        "Could not locate the leader master");

  (*c->data_)->meta_cache_.reset(new internal::MetaCache(c->data_->get()));
  (*c->data_)->dns_resolver_.reset(new DnsResolver());

  // Init local host names used for locality decisions.
  RETURN_NOT_OK_PREPEND(c->data_->get()->InitLocalHostNames(),
                        "Could not determine local host names");

  client->swap(c);
  return Status::OK();
}

} // namespace client
} // namespace kudu
