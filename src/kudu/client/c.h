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

#include "stddef.h"
#include "stdint.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct kudu_client_builder_t kudu_client_builder_t;
typedef struct kudu_client_t kudu_client_t;
typedef struct kudu_status_t kudu_status_t;
typedef struct kudu_table_list_t kudu_table_list_t;

////////////////////////////////////////////////////////////////////////////////
// Kudu Status
//
// Kudu Status represents the result of an operation which may fail. Operations
// which may fail can return a Kudu Status to indicate the failure, or a null
// pointer to indicate no failure.
////////////////////////////////////////////////////////////////////////////////

// Kudu Status instances must be destroyed if they are not OK.
void kudu_status_destroy(const kudu_status_t*);

// Get the Kudu error code associated with the Kudu Status.
int8_t kudu_status_code(const kudu_status_t*);

// Get the POSIX error code associated with the Kudu Status.
int16_t kudu_status_posix_code(const kudu_status_t*);

// Get the error message associated with the Kudu Status.
// The message is valid for the lifetime of the Kudu Status.
const char* kudu_status_message(const kudu_status_t*, size_t* len);

////////////////////////////////////////////////////////////////////////////////
// Kudu Client Builder
//
// Kudu Client Builder manages the cluster configuration necessary to connect to
// a Kudu cluster and create a client.
////////////////////////////////////////////////////////////////////////////////

// Creates a new Kudu Client Builder. Must be destroyed with
// kudu_client_builder_destroy when no longer needed.
kudu_client_builder_t* kudu_client_builder_create();

// Destroys the Kudu Client Builder.
void kudu_client_builder_destroy(kudu_client_builder_t*);

// Adds the master with the provided RPC address to the cluster configuration.
// The Client Builder does *not* take ownership of the address.
void kudu_client_builder_add_master_server_addr(kudu_client_builder_t*,
                                                const char* addr);

// Clears the cluster configuration of master addresses.
void kudu_client_builder_clear_master_server_addrs(kudu_client_builder_t*);

// Sets the default timeout used for administrative operations (e.g.
// CreateTable, AlterTable, ...). Optional.
//
// If not provided, defaults to 10 seconds.
void kudu_client_builder_set_default_admin_operation_timeout(kudu_client_builder_t*,
                                                             int64_t timeout_millis);

// Sets the default timeout for individual RPCs. Optional.
//
// If not provided, defaults to 5 seconds.
void kudu_client_builder_set_default_rpc_timeout(kudu_client_builder_t*, int64_t timeout_millis);

// Creates the client.
//
// The return value may indicate an error in the create operation, or a misuse
// of the builder; in the latter case, only the last error is returned.
const kudu_status_t* kudu_client_builder_build(kudu_client_builder_t*,
                                               kudu_client_t** client);

////////////////////////////////////////////////////////////////////////////////
// Kudu Table List
////////////////////////////////////////////////////////////////////////////////

void kudu_table_list_destroy(kudu_table_list_t*);

// Returns the number of tables.
size_t kudu_table_list_size(const kudu_table_list_t*);

// Returns the null-terminated name of the table in the list. The name is valid
// for the lifetime of the Kudu Table List.
const char* kudu_table_list_table_name(const kudu_table_list_t*, size_t index);

////////////////////////////////////////////////////////////////////////////////
// Kudu Client
//
// The Kudu Client represents a connection to a cluster. From the user
// perspective, they should only need to create one of these in their
// application, likely a singleton -- but it's not a singleton in Kudu in any
// way. Different Client objects do not interact with each other -- no
// connection pooling, etc. Each Kudu Client instance is sandboxed with no
// global cross-client state.
//
// In the implementation, the client holds various pieces of common
// infrastructure which is not table-specific:
//
// - RPC messenger: reactor threads and RPC connections are pooled here
// - Authentication: the client is initialized with some credentials, and
//   all accesses through it share those credentials.
// - Caches: caches of table schemas, tablet locations, tablet server IP
//   addresses, etc are shared per-client.
//
// In order to actually access data on the cluster, callers must first create a
// Kudu Session using kudu_client_new_session(). A Kudu Client may have several
// associated sessions.
////////////////////////////////////////////////////////////////////////////////

// Destroys the Kudu Client.
void kudu_client_destroy(kudu_client_t*);

// Returns the tables.
const kudu_status_t* kudu_client_list_tables(const kudu_client_t*, kudu_table_list_t** tables);

#ifdef __cplusplus
} // extern "C"
#endif
