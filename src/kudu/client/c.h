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

typedef struct kudu_client kudu_client;
typedef struct kudu_client_builder kudu_client_builder;
typedef struct kudu_column_schema kudu_column_schema;
typedef struct kudu_column_schema_builder kudu_column_schema_builder;
typedef struct kudu_partial_row kudu_partial_row;
typedef struct kudu_schema kudu_schema;
typedef struct kudu_schema_builder kudu_schema_builder;
typedef struct kudu_status kudu_status;
typedef struct kudu_table_creator kudu_table_creator;
typedef struct kudu_table_list kudu_table_list;
typedef struct kudu_tablet_server kudu_tablet_server;
typedef struct kudu_tablet_server_list kudu_tablet_server_list;

typedef enum kudu_data_type {
  KUDU_INT8 = 0,
  KUDU_INT16 = 1,
  KUDU_INT32 = 2,
  KUDU_INT64 = 3,
  KUDU_STRING = 4,
  KUDU_BOOL = 5,
  KUDU_FLOAT = 6,
  KUDU_DOUBLE = 7,
  KUDU_BINARY = 8,
  KUDU_TIMESTAMP = 9
} kudu_data_type;

typedef enum kudu_compression_type {
  KUDU_DEFAULT_COMPRESSION = 0,
  KUDU_NO_COMPRESSION = 1,
  KUDU_SNAPPY_COMPRESSION = 2,
  KUDU_LZ4_COMPRESSION = 3,
  KUDU_ZLIB_COMPRESSION = 4,
} kudu_compression_type;

typedef enum kudu_encoding_type {
    KUDU_DEFAULT_ENCODING = 0,
    KUDU_PLAIN_ENCODING = 1,
    KUDU_PREFIX_ENCODING = 2,
    KUDU_GROUP_VARINT_ENCODING = 3,
    KUDU_RUN_LENGTH_ENCODING = 4,
    KUDU_DICT_ENCODING = 5,
    KUDU_BIT_SHUFFLE_ENCODING = 6
} kudu_encoding_type;

// An immutable (const) reference to a chunk of data with a fixed length.
//
// The lifetime of the slice and the format of the data (e.g. UTF-8) should be
// specified in the interface which produces or consumes the slice. Unless
// otherwise specified, functions which take a slice as an argument do not take
// ownership of the data, and do not require that the data live longer than the
// function call. Functions which return a slice should document the lifetime of
// the data.
//
// The data pointed to by the slice must not be modified during the lifetime of
// the slice.
//
// Slice instances may be freely copied, but copies must not outlive the
// original slice.
//
// The data need not be null terminated.
typedef struct kudu_slice {
  const uint8_t* data;
  size_t len;
} kudu_slice;

// An array of Kudu Slices and the length of the array.
//
// The same lifetime and mutability restrictions that apply to Kudu Slice
// instances also apply to Kudu Slice List instances.
typedef struct kudu_slice_list {
  const kudu_slice* data;
  size_t len;
} kudu_slice_list;

////////////////////////////////////////////////////////////////////////////////
// Kudu Status
//
// Kudu Status represents the result of an operation which may fail. Operations
// which may fail can return a Kudu Status to indicate the failure, or a null
// pointer to indicate no failure.
////////////////////////////////////////////////////////////////////////////////

// Kudu Status instances must be destroyed if they are not OK.
void kudu_status_destroy(kudu_status*);

// Get the Kudu error code associated with the Kudu Status.
int8_t kudu_status_code(const kudu_status*);

// Get the POSIX error code associated with the Kudu Status.
int16_t kudu_status_posix_code(const kudu_status*);

// Get the error message associated with the Kudu Status as a UTF-8 encoded
// string slice. The message is valid for the lifetime of the Kudu Status.
kudu_slice kudu_status_message(const kudu_status*);

////////////////////////////////////////////////////////////////////////////////
// Kudu Client Builder
//
// Kudu Client Builder manages the cluster configuration necessary to connect to
// a Kudu cluster and create a client.
////////////////////////////////////////////////////////////////////////////////

// Creates a new Kudu Client Builder. Must be destroyed with
// kudu_client_builder_destroy when no longer needed.
kudu_client_builder* kudu_client_builder_create();

// Destroys the Kudu Client Builder.
void kudu_client_builder_destroy(kudu_client_builder*);

// Adds the master with the provided RPC address to the cluster configuration.
void kudu_client_builder_add_master_server_addr(kudu_client_builder*, kudu_slice addr);

// Clears the cluster configuration of master addresses.
void kudu_client_builder_clear_master_server_addrs(kudu_client_builder*);

// Sets the default timeout used for administrative operations (e.g.
// CreateTable, AlterTable, ...). Optional.
//
// If not provided, defaults to 10 seconds.
void kudu_client_builder_set_default_admin_operation_timeout(kudu_client_builder*,
                                                             int64_t timeout_millis);

// Sets the default timeout for individual RPCs. Optional.
//
// If not provided, defaults to 5 seconds.
void kudu_client_builder_set_default_rpc_timeout(kudu_client_builder*, int64_t timeout_millis);

// Creates the client.
//
// The return value may indicate an error in the create operation, or a misuse
// of the builder; in the latter case, only the last error is returned.
kudu_status* kudu_client_builder_build(kudu_client_builder*, kudu_client**const client);

////////////////////////////////////////////////////////////////////////////////
// Kudu Table List
////////////////////////////////////////////////////////////////////////////////

void kudu_table_list_destroy(kudu_table_list*);

// Returns the number of tables.
size_t kudu_table_list_size(const kudu_table_list*);

// Returns the name of the table in the list as a UTF-8 encoded string slice.
// The returned slice is valid for the lifetime of the table list.
kudu_slice kudu_table_list_table_name(const kudu_table_list*, size_t index);

////////////////////////////////////////////////////////////////////////////////
// Kudu Tablet Server List
////////////////////////////////////////////////////////////////////////////////

void kudu_tablet_server_list_destroy(kudu_tablet_server_list*);

// Returns the number of tablet servers in the list.
size_t kudu_tablet_server_list_size(const kudu_tablet_server_list*);

// Returns the tablet server at the provided index in the list.
const kudu_tablet_server* kudu_tablet_server_list_get(const kudu_tablet_server_list*, size_t idx);

////////////////////////////////////////////////////////////////////////////////
// Kudu Tablet Server
////////////////////////////////////////////////////////////////////////////////

// Returns the hostname of the tablet server.
// The returned slice is valid for the lifetime of the tablet server.
kudu_slice kudu_tablet_server_hostname(const kudu_tablet_server*);

// Return the UUID of the tablet server.
// The returned slice is valid for the lifetime of the tablet server.
kudu_slice kudu_tablet_server_uuid(const kudu_tablet_server*);

////////////////////////////////////////////////////////////////////////////////
// Kudu Schema
////////////////////////////////////////////////////////////////////////////////

void kudu_schema_destroy(kudu_schema*);

// Returns the number of columns in the schema.
size_t kudu_schema_num_columns(const kudu_schema*);

// Returns the number of primary key columns in the schema.
size_t kudu_schema_num_primary_key_columns(const kudu_schema*);

// Returns the column schema for the column at the specified index.
//
// TODO: this should return a kudu_status, since it can fail.
kudu_column_schema* kudu_schema_column(const kudu_schema*, size_t idx);

// Returns the index of the column in `idx`, or returns an error status if the
// column is not found.
kudu_status* kudu_schema_find_column(const kudu_schema*,
                                     kudu_slice column_name,
                                     size_t* idx);

// Returns a new row corresponding to this schema.
//
// The new row refers to this schema object, so must be destroyed before the
// schema object.
//
// The caller takes ownership of the created row.
kudu_partial_row* kudu_schema_new_row(const kudu_schema*);

////////////////////////////////////////////////////////////////////////////////
// Kudu Column Schema
////////////////////////////////////////////////////////////////////////////////

void kudu_column_schema_destroy(kudu_column_schema*);

// Returns the name of the column as a UTF-8 encoded string slice.
// The returned slice is valid for the lifetime of the column schema.
kudu_slice kudu_column_schema_name(const kudu_column_schema*);

// Returns true if the column is nullable.
int32_t kudu_column_schema_is_nullable(const kudu_column_schema*);

// Returns the type of the column.
kudu_data_type kudu_column_schema_data_type(const kudu_column_schema*);

kudu_encoding_type kudu_column_schema_encoding_type(const kudu_column_schema*);

kudu_compression_type kudu_column_schema_compression_type(const kudu_column_schema*);

////////////////////////////////////////////////////////////////////////////////
// Kudu Schema Builder
////////////////////////////////////////////////////////////////////////////////

// Creates a new schema builder.
kudu_schema_builder* kudu_schema_builder_create();

// Destroys a schema builder.
void kudu_schema_builder_destroy(kudu_schema_builder*);

// Adds a column to the schema. The returned column schema builder is valid for
// the lifetime of the schema builder.
kudu_column_schema_builder* kudu_schema_builder_add_column(kudu_schema_builder*, kudu_slice name);

// Sets the primary key of the schema to the columns
void kudu_schema_builder_set_primary_key_columns(kudu_schema_builder*,
                                                 kudu_slice_list column_names);

// Builds the schema.
kudu_status* kudu_schema_builder_build(kudu_schema_builder*, kudu_schema**const schema);

////////////////////////////////////////////////////////////////////////////////
// Kudu Column Schema Builder
////////////////////////////////////////////////////////////////////////////////

// Sets the column data type.
void kudu_column_schema_builder_data_type(kudu_column_schema_builder*, kudu_data_type);

// Sets the column encoding type.
void kudu_column_schema_builder_encoding_type(kudu_column_schema_builder*, kudu_encoding_type);

// Sets the column compression type.
void kudu_column_schema_builder_compression_type(kudu_column_schema_builder*,
                                                 kudu_compression_type);

// Sets the column block size.
void kudu_column_schema_builder_block_size(kudu_column_schema_builder*, int32_t);

// Sets the column to be nullable or non-nullable.
void kudu_column_schema_builder_nullable(kudu_column_schema_builder*, int32_t/*bool*/ nullable);

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
void kudu_client_destroy(kudu_client*);

// Creates a new table creator.
kudu_table_creator* kudu_client_new_table_creator(kudu_client*);

// Drops the table.
kudu_status* kudu_client_delete_table(kudu_client*, kudu_slice table_name);

// Retrieves the schema for the table with the given name storing the result in
// schema, or returns an error status.
kudu_status* kudu_client_get_table_schema(kudu_client*, kudu_slice table_name, kudu_schema**const schema);

// Returns the list of tables in the Kudu cluster whose names pass a substring
// match on 'filter' storing the result in tables, or returns an error status.
kudu_status* kudu_client_list_tables(kudu_client*, kudu_slice filter, kudu_table_list**const tables);

// Return the list of tablet servers in the Kudu cluster, or returns an error
// status.
kudu_status* kudu_client_list_tablet_servers(kudu_client*, kudu_tablet_server_list**const tservers);

////////////////////////////////////////////////////////////////////////////////
// Kudu Table Creator
////////////////////////////////////////////////////////////////////////////////

void kudu_table_creator_destroy(kudu_table_creator*);

// Sets the new tables name. Required.
void kudu_table_creator_table_name(kudu_table_creator*, kudu_slice table_name);

// Sets the table schema. The caller retains ownership of the schema. The schema
// must outlive the table creator. Required.
void kudu_table_creator_schema(kudu_table_creator*, const kudu_schema* schema);

void kudu_table_creator_add_hash_partitions(kudu_table_creator*,
                                            kudu_slice_list columns,
                                            int32_t num_buckets,
                                            int32_t seed);

void kudu_table_creator_set_range_partition_columns(kudu_table_creator*, kudu_slice_list columns);

void kudu_table_creator_add_split_row(kudu_table_creator*, kudu_partial_row* split_row);

void kudu_table_creator_num_replicas(kudu_table_creator*, int32_t num_replicas);

void kudu_table_creator_timeout(kudu_table_creator*, int64_t timeout_ms);

void kudu_table_creator_wait(kudu_table_creator*, int32_t/*bool*/ wait);

kudu_status* kudu_table_creator_create(kudu_table_creator*);

////////////////////////////////////////////////////////////////////////////////
// Kudu Partial Row
////////////////////////////////////////////////////////////////////////////////

void kudu_partial_row_destroy(kudu_partial_row*);

int32_t/*bool*/ kudu_partial_row_is_key_set(const kudu_partial_row*);
int32_t/*bool*/ kudu_partial_row_all_columns_set(const kudu_partial_row*);

kudu_status* kudu_partial_row_set_bool_by_name(kudu_partial_row*,
                                               kudu_slice column_name,
                                               int32_t/*bool*/ val);
kudu_status* kudu_partial_row_set_int8_by_name(kudu_partial_row*,
                                               kudu_slice column_name,
                                               int8_t val);
kudu_status* kudu_partial_row_set_int16_by_name(kudu_partial_row*,
                                                kudu_slice column_name,
                                                int16_t val);
kudu_status* kudu_partial_row_set_int32_by_name(kudu_partial_row*,
                                                kudu_slice column_name,
                                                int32_t val);
kudu_status* kudu_partial_row_set_int64_by_name(kudu_partial_row*,
                                                kudu_slice column_name,
                                                int64_t val);
kudu_status* kudu_partial_row_set_timestamp_by_name(kudu_partial_row*,
                                                    kudu_slice column_name,
                                                    int64_t val);
kudu_status* kudu_partial_row_set_float_by_name(kudu_partial_row*,
                                                kudu_slice column_name,
                                                float val);
kudu_status* kudu_partial_row_set_double_by_name(kudu_partial_row*,
                                                 kudu_slice column_name,
                                                 double val);
kudu_status* kudu_partial_row_set_string_by_name(kudu_partial_row*,
                                                 kudu_slice column_name,
                                                 kudu_slice val);
kudu_status* kudu_partial_row_set_string_copy_by_name(kudu_partial_row*,
                                                      kudu_slice column_name,
                                                      kudu_slice val);
kudu_status* kudu_partial_row_set_binary_by_name(kudu_partial_row*,
                                                 kudu_slice column_name,
                                                 kudu_slice val);
kudu_status* kudu_partial_row_set_binary_copy_by_name(kudu_partial_row*,
                                                      kudu_slice column_name,
                                                      kudu_slice val);
kudu_status* kudu_partial_row_set_null_by_name(kudu_partial_row*, kudu_slice column_name);
kudu_status* kudu_partial_row_unset_by_name(kudu_partial_row*, kudu_slice column_name);

kudu_status* kudu_partial_row_set_bool(kudu_partial_row*,
                                       size_t column_idx,
                                       int32_t/*bool*/ val);
kudu_status* kudu_partial_row_set_int8(kudu_partial_row*,
                                       size_t column_idx,
                                       int8_t val);
kudu_status* kudu_partial_row_set_int16(kudu_partial_row*,
                                        size_t column_idx,
                                        int16_t val);
kudu_status* kudu_partial_row_set_int32(kudu_partial_row*,
                                        size_t column_idx,
                                        int32_t val);
kudu_status* kudu_partial_row_set_int64(kudu_partial_row*,
                                        size_t column_idx,
                                        int64_t val);
kudu_status* kudu_partial_row_set_timestamp(kudu_partial_row*,
                                            size_t column_idx,
                                            int64_t val);
kudu_status* kudu_partial_row_set_float(kudu_partial_row*,
                                        size_t column_idx,
                                        float val);
kudu_status* kudu_partial_row_set_double(kudu_partial_row*,
                                         size_t column_idx,
                                         double val);
kudu_status* kudu_partial_row_set_string(kudu_partial_row*,
                                         size_t column_idx,
                                         kudu_slice val);
kudu_status* kudu_partial_row_set_string_copy(kudu_partial_row*,
                                              size_t column_idx,
                                              kudu_slice val);
kudu_status* kudu_partial_row_set_binary(kudu_partial_row*,
                                         size_t column_idx,
                                         kudu_slice val);
kudu_status* kudu_partial_row_set_binary_copy(kudu_partial_row*,
                                              size_t column_idx,
                                              kudu_slice val);
kudu_status* kudu_partial_row_set_null(kudu_partial_row*, size_t column_idx);
kudu_status* kudu_partial_row_unset(kudu_partial_row*, size_t column_idx);

kudu_status* kudu_partial_row_get_bool_by_name(const kudu_partial_row*,
                                               kudu_slice column_name,
                                               int32_t/*bool*/* val);
kudu_status* kudu_partial_row_get_int8_by_name(const kudu_partial_row*,
                                               kudu_slice column_name,
                                               int8_t* val);
kudu_status* kudu_partial_row_get_int16_by_name(const kudu_partial_row*,
                                                kudu_slice column_name,
                                                int16_t* val);
kudu_status* kudu_partial_row_get_int32_by_name(const kudu_partial_row*,
                                                kudu_slice column_name,
                                                int32_t* val);
kudu_status* kudu_partial_row_get_int64_by_name(const kudu_partial_row*,
                                                kudu_slice column_name,
                                                int64_t* val);
kudu_status* kudu_partial_row_get_timestamp_by_name(const kudu_partial_row*,
                                                    kudu_slice column_name,
                                                    int64_t* val);
kudu_status* kudu_partial_row_get_float_by_name(const kudu_partial_row*,
                                                kudu_slice column_name,
                                                float* val);
kudu_status* kudu_partial_row_get_double_by_name(const kudu_partial_row*,
                                                 kudu_slice column_name,
                                                 double* val);
kudu_status* kudu_partial_row_get_string_by_name(const kudu_partial_row*,
                                                 kudu_slice column_name,
                                                 kudu_slice* val);
kudu_status* kudu_partial_row_get_binary_by_name(const kudu_partial_row*,
                                                 kudu_slice column_name,
                                                 kudu_slice* val);
int32_t/*bool*/ kudu_partial_row_is_null_by_name(const kudu_partial_row*, kudu_slice column_name);
int32_t/*bool*/ kudu_partial_row_is_set_by_name(const kudu_partial_row*, kudu_slice column_name);

kudu_status* kudu_partial_row_get_bool(const kudu_partial_row*,
                                       size_t column_idx,
                                       int32_t/*bool*/* val);
kudu_status* kudu_partial_row_get_int8(const kudu_partial_row*,
                                       size_t column_idx,
                                       int8_t* val);
kudu_status* kudu_partial_row_get_int16(const kudu_partial_row*,
                                        size_t column_idx,
                                        int16_t* val);
kudu_status* kudu_partial_row_get_int32(const kudu_partial_row*,
                                        size_t column_idx,
                                        int32_t* val);
kudu_status* kudu_partial_row_get_int64(const kudu_partial_row*,
                                        size_t column_idx,
                                        int64_t* val);
kudu_status* kudu_partial_row_get_timestamp(const kudu_partial_row*,
                                            size_t column_idx,
                                            int64_t* val);
kudu_status* kudu_partial_row_get_float(const kudu_partial_row*,
                                        size_t column_idx,
                                        float* val);
kudu_status* kudu_partial_row_get_double(const kudu_partial_row*,
                                         size_t column_idx,
                                         double* val);
kudu_status* kudu_partial_row_get_string(const kudu_partial_row*,
                                         size_t column_idx,
                                         kudu_slice* val);
kudu_status* kudu_partial_row_get_binary(const kudu_partial_row*,
                                         size_t column_idx,
                                         kudu_slice* val);
int32_t/*bool*/ kudu_partial_row_is_null(const kudu_partial_row*, size_t column_idx);
int32_t/*bool*/ kudu_partial_row_is_set(const kudu_partial_row*, size_t column_idx);

#ifdef __cplusplus
} // extern "C"
#endif
