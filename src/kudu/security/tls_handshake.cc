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
#include <memory>

#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include "kudu/security/tls_socket.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"

#if OPENSSL_VERSION_NUMBER < 0x10002000L
#include "kudu/security/x509_check_host.h"
#endif // OPENSSL_VERSION_NUMBER

namespace kudu {
namespace security {

Status TlsHandshake::Continue(const string& recv, string* send) {
  CHECK_NOTNULL(ssl_);
  ERR_clear_error();

  int n = BIO_write(rbio_, recv.data(), recv.size());
  DCHECK_EQ(n, recv.size());
  DCHECK_EQ(BIO_ctrl_pending(rbio_), recv.size());

  int rc = SSL_do_handshake(ssl_);
  if (rc != 1) {
    int ssl_err = SSL_get_error(ssl_, rc);
    // WANT_READ and WANT_WRITE indicate that the handshake is not yet complete.
    if (ssl_err != SSL_ERROR_WANT_READ && ssl_err != SSL_ERROR_WANT_WRITE) {
      return Status::NetworkError("SSL Handshake error", GetSSLErrorDescription(ssl_err));
    }
  }

  int pending = BIO_ctrl_pending(wbio_);

  send->resize(pending);
  BIO_read(wbio_, &(*send)[0], send->size());
  DCHECK_EQ(BIO_ctrl_pending(wbio_), 0);

  if (rc == 1) {
    // The handshake is done, but in the case of the server, we still need to
    // send the final response to the client.
    DCHECK_GE(send->size(), 0);
    return Status::OK();
  } else {
    DCHECK_GT(send->size(), 0);
    return Status::Incomplete("TLS Handshake incomplete");
  }
}

bool TlsHandshake::ShouldContinue() const {
  CHECK_NOTNULL(ssl_);
  return !SSL_is_init_finished(ssl_);
}

Status TlsHandshake::Verify(const Socket& socket) const {
  DCHECK(!ShouldContinue());
  CHECK_NOTNULL(ssl_);
  ERR_clear_error();

  // Verify if the handshake was successful.
  int rc = SSL_get_verify_result(ssl_);
  if (rc != X509_V_OK) {
    return Status::NetworkError("SSL_get_verify_result()", X509_verify_cert_error_string(rc));
  }

  // Get the peer certificate.
  std::unique_ptr<X509, void(*)(X509*)> cert(SSL_get_peer_certificate(ssl_),
                                             [] (X509* x) { X509_free(x); });
  if (cert == nullptr) {
    if (SSL_get_verify_mode(ssl_) & SSL_VERIFY_FAIL_IF_NO_PEER_CERT) {
      return Status::NetworkError("Handshake failed: Could not retreive peer certificate");
    }
  }

  // Get the peer's hostname
  Sockaddr peer_addr;
  if (!socket.GetPeerAddress(&peer_addr).ok()) {
    return Status::NetworkError("Handshake failed: Could not retrieve peer address");
  }
  std::string peer_hostname;
  RETURN_NOT_OK(peer_addr.LookupHostname(&peer_hostname));

  // Check if the hostname matches with either the Common Name or any of the Subject Alternative
  // Names of the certificate.
  int match = X509_check_host(cert.get(),
                              peer_hostname.c_str(),
                              peer_hostname.length(),
                              0,
                              nullptr);
  if (match == 0) {
    return Status::NetworkError("Handshake failed: Could not verify host with certificate");
  } else if (match < 0) {
    return Status::NetworkError("Handshake failed", GetOpenSSLErrors());
  }
  DCHECK_EQ(match, 1);
  return Status::OK();
}

Status TlsHandshake::Finish(std::unique_ptr<Socket>* socket) {
  RETURN_NOT_OK(Verify(**socket));

  int fd = (*socket)->Release();
  int ret = SSL_set_fd(ssl_, fd);
  if (ret != 1) {
    return Status::NetworkError("SSL Handshake error", GetOpenSSLErrors());
  }

  socket->reset(new TlsSocket(fd, ssl_));

  // The SSL handle has been transferred to the socket.
  ssl_ = nullptr;
  return Status::OK();
}

} // namespace security
} // namespace kudu
