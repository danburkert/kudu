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

// A KRPC server which implements the CalculatorService (rtest.proto). This is
// primarily intended for testing KRPC client implementations in other
// languages.

#include <memory>
#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc-test-base.h"
#include "kudu/util/logging.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/net/sockaddr.h"

DEFINE_string(addr, "0.0.0.0:0",
              "The interface and port to bind the calculator service to.");
DEFINE_bool(enable_tls, false,
            "Whether to enable SSL/TLS on the calculator server.");

using std::shared_ptr;
using std::string;

namespace kudu {
namespace rpc {

Sockaddr StartCalculatorServer() {
  Sockaddr addr;
  CHECK_OK(addr.ParseString(FLAGS_addr, 0));

  // Create the metric registry.
  MetricRegistry metric_registry;
  scoped_refptr<MetricEntity> metric_entity(METRIC_ENTITY_server.Instantiate(&metric_registry,
                                                                             "calculator-server"));

  // Create the messenger.
  MessengerBuilder messenger_builder("calculator-server");
  if (FLAGS_enable_tls) {
    messenger_builder.enable_inbound_tls();
  }
  messenger_builder.set_metric_entity(metric_entity);
  shared_ptr<Messenger> messenger;
  messenger_builder.Build(&messenger);

  // Create the acceptor pool.
  shared_ptr<AcceptorPool> pool;
  CHECK_OK(messenger->AddAcceptorPool(addr, &pool));
  CHECK_OK(pool->Start(2));

  Sockaddr bind_address = pool->bind_address();

  // Create the memory and result tracker.
  shared_ptr<MemTracker> mem_tracker = MemTracker::CreateTracker(-1, "result_tracker");
  scoped_refptr<ResultTracker> result_tracker(new ResultTracker(mem_tracker));

  gscoped_ptr<ServiceIf> service(new CalculatorService(metric_entity, result_tracker));
  string service_name = service->service_name();

  scoped_refptr<ServicePool> service_pool(new ServicePool(std::move(service),
                                                          metric_entity,
                                                          100));
  messenger->RegisterService(service_name, service_pool);
  CHECK_OK(service_pool->Init(2));

  return bind_address;
}

} // namespace rpc
} // namespace kudu

int main(int argc, char** argv) {
  FLAGS_logtostderr = true;
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  kudu::InitGoogleLoggingSafe(argv[0]);

  kudu::Sockaddr addr = kudu::rpc::StartCalculatorServer();
  LOG(INFO) << "calculator server bound to " << addr.ToString();
  SleepFor(kudu::MonoDelta::FromSeconds(15 * 60));
}
