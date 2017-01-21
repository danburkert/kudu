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

// Forward declarations for the OpenSSL typedefs.
typedef struct bio_st BIO;
typedef struct ssl_ctx_st SSL_CTX;
typedef struct ssl_st SSL;

namespace kudu {
namespace security {

// Initializes static state required by the OpenSSL library.
//
// Safe to call multiple times.
void InitializeOpenSSL();

// Fetches errors from the OpenSSL error error queue, and stringifies them.
//
// The error queue will be empty after this method returns.
//
// See man(3) ERR_get_err for more discussion.
std::string GetOpenSSLErrors();

// Returns a string representation of the provided error code, which must be
// from a prior call to the SSL_get_error function.
//
// If necessary, the OpenSSL error queue may be inspected and emptied as part of
// this call, and/or 'errno' may be inspected. As a result, this method should
// only be used directly after the error occurs, and from the same thread.
//
// See man(3) SSL_get_error for more discussion.
std::string GetSSLErrorDescription(int error_code);

} // namespace security
} // namespace kudu
