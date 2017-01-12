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

#include "kudu/security/tls_handshake.h"

#include <string>

#include <openssl/err.h>
#include <openssl/ssl.h>

#include "kudu/util/status.h"

namespace kudu {

Status TlsHandshake::Continue(const string& recv, string* send) {
  CHECK_NOTNULL(ssl_);
  ERR_clear_error();

  int n = BIO_write(rbio_, recv.data(), recv.size());
  DCHECK_EQ(n, recv.size());
  DCHECK_EQ(BIO_ctrl_pending(rbio_), recv.size());

  int rc = SSL_do_handshake(ssl_);
  if (rc == 1) {
    return Status::OK();
  } else {
    int ssl_err = SSL_get_error(ssl_, rc);
    if (ssl_err != SSL_ERROR_WANT_READ && ssl_err != SSL_ERROR_WANT_WRITE) {
      ERR_print_errors_fp(stderr);
      //return Status::NetworkError("SSL Handshake error", ERR_reason_error_string(ERR_get_error()));
      return Status::NetworkError("SSL Handshake error");
    }
  }

  int pending = BIO_ctrl_pending(wbio_);
  DCHECK_GT(pending, 0);
  DCHECK_LT(pending, 4096);

  send->resize(pending);
  BIO_read(wbio_, &(*send)[0], send->size());
  DCHECK_EQ(BIO_ctrl_pending(wbio_), 0);

  return Status::Incomplete("TLS Handshake incomplete");
}

}
