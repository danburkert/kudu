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

#include <string>

#include "kudu/rpc/messenger.h"
#include "kudu/server/rpc_server.h"
#include "kudu/util/test_util.h"

using std::string;

namespace kudu {

class RpcServerAdvertisedAddressesTest : public KuduTest {
 public:
  RpcServerAdvertisedAddressesTest() {
  }

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();

    RpcServerOptions opts;
    string bind = use_bind_addresses();
    string advertised = use_advertised_addresses();
    if (!bind.empty()) {
      opts.rpc_bind_addresses = bind;
    } else {
      opts.rpc_bind_addresses = "127.0.0.1:9898";
    }
    if (!advertised.empty()) {
      opts.rpc_advertised_addresses = advertised;
    }
    server_.reset(new RpcServer(opts));
    gscoped_ptr<MetricRegistry> metric_registry(new MetricRegistry());
    scoped_refptr<MetricEntity> metric_entity =
      METRIC_ENTITY_server.Instantiate(metric_registry.get(),
                                       "test");
    rpc::MessengerBuilder builder("test");
    std::shared_ptr<rpc::Messenger> messenger;
    builder.set_num_reactors(1)
      .set_min_negotiation_threads(1)
      .set_max_negotiation_threads(1)
      .set_metric_entity(metric_entity)
      .enable_inbound_tls();
    builder.Build(&messenger);
    server_->Init(messenger);
    server_->Bind();
  }

 protected:
  // Overridden by subclasses.
  virtual const string use_bind_addresses() const { return ""; }
  virtual const string use_advertised_addresses() const { return ""; }

  gscoped_ptr<RpcServer> server_;
};

class AdvertisedOnlyWebserverTest : public RpcServerAdvertisedAddressesTest {
 protected:
  const string use_advertised_addresses() const override { return "127.0.0.1:9999"; }
};

class BoundOnlyWebserverTest : public RpcServerAdvertisedAddressesTest {
 protected:
  const string use_bind_addresses() const override { return "127.0.0.1:9998"; }
};

class BothBoundAndAdvertisedWebserverTest : public RpcServerAdvertisedAddressesTest {
 protected:
  const string use_advertised_addresses() const override { return "127.0.0.1:9999"; }
  const string use_bind_addresses() const override { return "127.0.0.1:9998"; }
};

TEST_F(AdvertisedOnlyWebserverTest, OnlyAdvertisedAddresses) {
  vector<Sockaddr> advertised_addrs;
  ASSERT_OK(server_->GetAdvertisedAddresses(&advertised_addrs));
  ASSERT_EQ(advertised_addrs.size(), 1);
  vector<Sockaddr> bound_addrs;
  ASSERT_OK(server_->GetBoundAddresses(&bound_addrs));
  ASSERT_EQ(advertised_addrs[0].host(), "127.0.0.1");
  ASSERT_EQ(advertised_addrs[0].port(), 9999);
  ASSERT_NE(bound_addrs[0].port(), 9999);
}

TEST_F(BoundOnlyWebserverTest, OnlyBoundAddresses) {
  vector<Sockaddr> advertised_addrs;
  ASSERT_OK(server_->GetAdvertisedAddresses(&advertised_addrs));
  ASSERT_EQ(advertised_addrs.size(), 1);
  vector<Sockaddr> bound_addrs;
  ASSERT_OK(server_->GetBoundAddresses(&bound_addrs));
  ASSERT_EQ(bound_addrs.size(), 1);

  ASSERT_EQ(advertised_addrs[0].host(), "127.0.0.1");
  ASSERT_EQ(advertised_addrs[0].port(), 9998);
  ASSERT_EQ(bound_addrs[0].host(), "127.0.0.1");
  ASSERT_EQ(bound_addrs[0].port(), 9998);
}

TEST_F(BothBoundAndAdvertisedWebserverTest, BothBoundAndAdvertisedAddresses) {
  vector<Sockaddr> advertised_addrs;
  ASSERT_OK(server_->GetAdvertisedAddresses(&advertised_addrs));
  ASSERT_EQ(advertised_addrs.size(), 1);
  vector<Sockaddr> bound_addrs;
  ASSERT_OK(server_->GetBoundAddresses(&bound_addrs));
  ASSERT_EQ(bound_addrs.size(), 1);

  ASSERT_EQ(advertised_addrs[0].host(), "127.0.0.1");
  ASSERT_EQ(advertised_addrs[0].port(), 9999);
  ASSERT_EQ(bound_addrs[0].host(), "127.0.0.1");
  ASSERT_EQ(bound_addrs[0].port(), 9998);
}

} // namespace kudu
