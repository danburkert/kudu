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

class KuduScanToken::Data {
 public:
  explicit Data(KuduTable* table,
                ScanTokenPB message,
                std::vector<KuduTabletServer*> tablet_servers);
  ~Data();

  Status IntoKuduScanner(KuduScanner** scanner) const;

  const std::vector<KuduTabletServer*>& TabletServers() const {
    return tablet_servers_;
  }

  Status Serialize(std::string* buf) const;

  static Status DeserializeIntoScanner(KuduClient* client,
                                       const std::string& serialized_token,
                                       KuduScanner** scanner);

 private:

  static Status PBIntoScanner(KuduClient* client,
                              const ScanTokenPB& message,
                              KuduScanner** scanner);

  KuduTable* table_;
  ScanTokenPB message_;
  std::vector<KuduTabletServer*> tablet_servers_;
};

class KuduScanTokenBuilder::Data {
 public:
  explicit Data(KuduTable* table);
  ~Data() = default;

  Status Build(std::vector<KuduScanToken*>* tokens);

  const ScanConfiguration& configuration() const {
    return configuration_;
  }

  ScanConfiguration* mutable_configuration() {
    return &configuration_;
  }

 private:
  ScanConfiguration configuration_;
};

} // namespace client
} // namespace kudu
