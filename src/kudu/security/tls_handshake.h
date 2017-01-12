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

#include <string>

#include "kudu/util/status.h"

struct ssl_st;
typedef ssl_st SSL;

struct bio_st;
typedef bio_st BIO;

namespace kudu {

class TlsHandshake {
 public:
  TlsHandshake() = default;

  ~TlsHandshake() = default;

  // Continue the handshake.
  Status Continue(const std::string& recv, std::string* send);

 private:
  friend class SSLFactory;

  // Owned SSL handle.
  SSL* ssl_;
  BIO* rbio_;
  BIO* wbio_;

};

} // namespace kudu
