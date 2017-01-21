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

#include <memory>
#include <string>

#include "kudu/security/openssl_util.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/status.h"

namespace kudu {

class Socket;

namespace security {

// TlsHandshake manages an ongoing TLS handshake between a client and server.
//
// TlsHandshake instances are default constructed, but must be initialized
// before use using TlsContext::InitiateHandshake.
class TlsHandshake {
 public:

  // Continue or start a new handshake.
  //
  // 'recv' should contain the input buffer from the remote end, or and empty
  // string when the handshake is new.
  //
  // 'send' should contain the output buffer which must be sent to the remote
  // end.
  //
  // Returns Status::OK when the handshake is complete, however the 'send'
  // buffer may contain a message which must still be transmitted to the remote
  // end. If the send buffer is empty after this call and the return is
  // Status::OK, the socket should immediately be wrapped in the TLS channel
  // using 'Finish'. If the send buffer is not empty, the message should be sent
  // to the remote end, and then the socket should be wrapped using 'Finish'.
  //
  // Returns Status::Incomplete when the handshake must continue for another
  // round of messages.
  //
  // Returns any other status code on error.
  Status Continue(const std::string& recv, std::string* send);

  // Returns true if the handshake process should continue.
  bool ShouldContinue() const;

  // Finishes the handshake, wrapping the provided socket in the negotiated TLS
  // channel. This 'TlsHandshake' instance should not be used again after
  // calling this.
  Status Finish(std::unique_ptr<Socket>* socket);

 private:
  friend class TlsContext;

  // Verifies that the handshake is valid for the provided socket.
  Status Verify(const Socket& socket) const;

  // Owned SSL handle.
  SSL* ssl_;

  // Buffers owned by 'ssl_'.
  BIO* rbio_;
  BIO* wbio_;
};

} // namespace security
} // namespace kudu
