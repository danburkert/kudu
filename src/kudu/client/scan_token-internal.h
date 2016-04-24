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

#include <vector>
#include <string>

#include "kudu/client/client.h"
#include "kudu/client/client.pb.h"
#include "kudu/client/scan_configuration.h"

namespace kudu {
namespace client {
namespace internal {

class ScanToken {
 public:
  explicit ScanToken(Table* table,
                     ScanTokenPB message,
                     std::vector<TabletServer> tablet_servers);
  ~ScanToken() = default;

  Status IntoKuduScanner(KuduScanner** scanner) const;

  const std::vector<TabletServer>& TabletServers() const {
    return tablet_servers_;
  }

  Status Serialize(std::string* buf) const;

  static Status DeserializeIntoScanner(Client* client,
                                       const std::string& serialized_token,
                                       KuduScanner** scanner);

 private:

  static Status PBIntoScanner(Client* client,
                              const ScanTokenPB& message,
                              KuduScanner** scanner);

  Table* table_;
  ScanTokenPB message_;
  std::vector<TabletServer> tablet_servers_;
};

class ScanTokenBuilder {
 public:
  explicit ScanTokenBuilder(Table* table);
  ~ScanTokenBuilder() = default;

  Status Build(std::vector<KuduScanToken*>* tokens);

  const ScanConfiguration& configuration() const {
    return configuration_;
  }

  ScanConfiguration& configuration() {
    return configuration_;
  }

 private:
  ScanConfiguration configuration_;
};

} // namespace internal
} // namespace client
} // namespace kudu
