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

#include <gtest/gtest.h>

#include "kudu/rpc/rpc-test-base.h"
#include "kudu/util/net/ssl_factory.h"
#include "kudu/util/test_util.h"


namespace kudu {

class TestTlsHandshake : public KuduTest {};

TEST_F(TestTlsHandshake, TestHandshake) {
  string cert_path = GetTestPath("cert.pem");
  string key_path = GetTestPath("key.pem");
  CHECK_OK(rpc::CreateSSLServerCert(cert_path));
  CHECK_OK(rpc::CreateSSLPrivateKey(key_path));

  SSLFactory ssl_factory;
  ASSERT_OK(ssl_factory.Init());
  ASSERT_OK(ssl_factory.LoadCertificate(cert_path));
  ASSERT_OK(ssl_factory.LoadPrivateKey(key_path));
  ASSERT_OK(ssl_factory.LoadCertificateAuthority(cert_path));

  TlsHandshake server;
  TlsHandshake client;

  ssl_factory.InitiateHandshake(true, &server);
  ssl_factory.InitiateHandshake(false, &client);

  string buf1;
  string buf2;

  // Client Hello
  ASSERT_TRUE(client.Continue(buf1, &buf2).IsIncomplete());
  ASSERT_GT(buf2.size(), 0);

  // Server Hello
  ASSERT_TRUE(server.Continue(buf2, &buf1).IsIncomplete());
  ASSERT_GT(buf1.size(), 0);

  string buf3("foobarbaz");
  // Client Finished
  ASSERT_OK(server.Continue(buf1, &buf2));
  ASSERT_GT(buf2.size(), 0);

  // Server Finished
  ASSERT_OK(server.Continue(buf2, &buf1));
  ASSERT_GT(buf1.size(), 0);
}
} // namespace kudu
