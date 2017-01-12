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

#include "kudu/security/tls_common.h"

#include <mutex>
#include <sstream>
#include <string>

#include <openssl/err.h>
#include <openssl/rand.h>
#include <openssl/ssl.h>

#include "kudu/util/debug/leakcheck_disabler.h"
#include "kudu/util/mutex.h"
#include "kudu/util/thread.h"

namespace kudu {
namespace security {

// These non-POD elements will be alive for the lifetime of the process, so
// don't allocate in static storage.
static Mutex* g_ssl_mutexes;

// Lock/Unlock the nth lock. Only to be used by OpenSSL.
static void CryptoLockingCallback(int mode, int n, const char* /*unused*/, int /*unused*/) {
  if (mode & CRYPTO_LOCK) {
    g_ssl_mutexes[n].Acquire();
  } else {
    g_ssl_mutexes[n].Release();
  }
}

// Return the current pthread's tid. Only to be used by OpenSSL.
static void CryptoThreadIDCallback(CRYPTO_THREADID* id) {
  return CRYPTO_THREADID_set_numeric(id, Thread::UniqueThreadId());
}

static void DoInititializeTls() {
  SSL_library_init();
  SSL_load_error_strings();
  OpenSSL_add_all_algorithms();
  RAND_poll();

  debug::ScopedLeakCheckDisabler d;
  g_ssl_mutexes = new Mutex[CRYPTO_num_locks()];

  // Callbacks used by OpenSSL required in a multi-threaded setting.
  CRYPTO_set_locking_callback(CryptoLockingCallback);
  CRYPTO_THREADID_set_callback(CryptoThreadIDCallback);
}

void InitializeTls() {
  static std::once_flag ssl_once;
  std::call_once(ssl_once, DoInititializeTls);
}

std::string GetOpenSSLErrors() {
  ostringstream serr;
  uint32_t l;
  int line, flags;
  const char *file, *data;
  while ((l = ERR_get_error_line_data(&file, &line, &data, &flags)) != 0) {
    char buf[256];
    ERR_error_string_n(l, buf, sizeof(buf));
    serr << buf << ":" << file << ":" << line;
    if (flags & ERR_TXT_STRING) {
      serr << ":" << data;
    }
    serr << " ";
  }
  return serr.str();
}

} // namespace security
} // namespace kudu
