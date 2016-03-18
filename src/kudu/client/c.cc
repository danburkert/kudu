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

#include  "kudu/client/c.h"

#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/common/partial_row.h"
#include "kudu/util/monotime.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

using std::memcpy;
using std::move;
using std::string;
using std::unique_ptr;
using std::vector;

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduColumnSpec;
using kudu::client::KuduColumnStorageAttributes;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduTableCreator;
using kudu::client::KuduTabletServer;
using kudu::client::sp::shared_ptr;
using kudu::KuduPartialRow;
using kudu::MonoDelta;
using kudu::Slice;
using kudu::Status;

namespace {
  string slice_to_string(kudu_slice slice) {
    return string(reinterpret_cast<const char*>(slice.data), slice.len);
  }
  Slice slice_to_Slice(kudu_slice slice) {
    return Slice(slice.data, slice.len);
  }

  kudu_slice string_to_slice(const string& s) {
    return kudu_slice { .data = reinterpret_cast<const uint8_t*>(s.data()), .len = s.size() };
  }

  vector<string> slice_list_to_vector(kudu_slice_list list) {
    vector<string> v;
    v.reserve(list.len);
    for (int i = 0; i < list.len; i++) {
      v.emplace_back(slice_to_string(list.data[i]));
    }
    return v;
  }

  // kudu_table_creator, kudu_column_schema_builder, kudu_status, and
  // kudu_partial_row are all undefined types which are only used as an opaque
  // pointer to an internal class. The following helper functions take care of
  // the casts.

  KuduTableCreator* to_internal(kudu_table_creator* creator) {
      return reinterpret_cast<KuduTableCreator*>(creator);
  }
  KuduPartialRow* to_internal(kudu_partial_row* row) {
      return reinterpret_cast<KuduPartialRow*>(row);
  }
  const KuduPartialRow* to_internal(const kudu_partial_row* row) {
      return reinterpret_cast<const KuduPartialRow*>(row);
  }
  KuduColumnSpec* to_internal(kudu_column_schema_builder* builder) {
      return reinterpret_cast<KuduColumnSpec*>(builder);
  }
  const KuduTabletServer* to_internal(const kudu_tablet_server* tserver) {
    return reinterpret_cast<const KuduTabletServer*>(tserver);
  }
} // anonymous namespace

extern "C" {

struct kudu_client_builder { KuduClientBuilder builder_; };
struct kudu_client { shared_ptr<KuduClient> client_; };
struct kudu_schema { KuduSchema schema_; };
struct kudu_table_list { vector<string> list_; };
struct kudu_tablet_server_list { vector<KuduTabletServer*> list_; };

struct kudu_column_schema {
  kudu_column_schema(KuduColumnSchema column) : column_(move(column)) {}
  KuduColumnSchema column_;
};

struct kudu_schema_builder { KuduSchemaBuilder builder_; };

////////////////////////////////////////////////////////////////////////////////
// Kudu Status
//
// kudu_status is an alias to a const char* with the same internal format as
// Status::state_.
////////////////////////////////////////////////////////////////////////////////

void kudu_status_destroy(kudu_status* status) {
  delete[] reinterpret_cast<const char*>(status);
}

int8_t kudu_status_code(const kudu_status* status) {
  if (status == nullptr) {
    return 0;
  }
  return reinterpret_cast<const int8_t*>(status)[4];
}

int16_t kudu_status_posix_code(const kudu_status* status) {
  if (status == nullptr) {
    return 0;
  }
  return *reinterpret_cast<const int16_t*>(reinterpret_cast<const char*>(status)[5]);
}

kudu_slice kudu_status_message(const kudu_status* status) {
  if (status == nullptr) {
    return kudu_slice { .data = nullptr, .len = 0 };
  }
  const char* data = reinterpret_cast<const char*>(status);
  size_t len = static_cast<size_t>(*reinterpret_cast<const uint32_t*>(data));
  return kudu_slice { .data = reinterpret_cast<const uint8_t*>(&data[7]), .len = len };
}

////////////////////////////////////////////////////////////////////////////////
// Kudu Client Builder
////////////////////////////////////////////////////////////////////////////////

kudu_client_builder* kudu_client_builder_create() {
  return new kudu_client_builder();
}

void kudu_client_builder_destroy(kudu_client_builder* builder) {
  delete builder;
}

void kudu_client_builder_add_master_server_addr(kudu_client_builder* builder,
                                                kudu_slice addr) {
  builder->builder_.add_master_server_addr(slice_to_string(addr));
}

void kudu_client_builder_clear_master_server_addrs(kudu_client_builder* builder) {
  builder->builder_.clear_master_server_addrs();
}

void kudu_client_builder_set_default_admin_operation_timeout(kudu_client_builder* builder,
                                                             int64_t timeout_millis) {
  builder->builder_.default_admin_operation_timeout(MonoDelta::FromMilliseconds(timeout_millis));
}

void kudu_client_builder_set_default_rpc_timeout(kudu_client_builder* builder,
                                                 int64_t timeout_millis) {
  builder->builder_.default_rpc_timeout(MonoDelta::FromMilliseconds(timeout_millis));
}

kudu_status* kudu_client_builder_build(kudu_client_builder* builder,
                                       kudu_client**const client) {
  unique_ptr<kudu_client> c(new kudu_client);
  RETURN_NOT_OK_C(builder->builder_.Build(&c->client_));
  *client = c.release();
  return nullptr;
}

////////////////////////////////////////////////////////////////////////////////
// Kudu Table List
////////////////////////////////////////////////////////////////////////////////

void kudu_table_list_destroy(kudu_table_list* list) {
  delete list;
}

size_t kudu_table_list_size(const kudu_table_list* list) {
  return list->list_.size();
}

// Returns the null-terminated name of the table in the list. The name is valid
// for the lifetime of the Kudu Table List.
kudu_slice kudu_table_list_table_name(const kudu_table_list* list, size_t index) {
  CHECK(index < list->list_.size());
  return string_to_slice(list->list_[index]);
}

////////////////////////////////////////////////////////////////////////////////
// Kudu Tablet Server
////////////////////////////////////////////////////////////////////////////////

kudu_slice kudu_tablet_server_hostname(const kudu_tablet_server* tserver) {
  return string_to_slice(to_internal(tserver)->hostname());
}

kudu_slice kudu_tablet_server_uuid(const kudu_tablet_server* tserver) {
  return string_to_slice(to_internal(tserver)->uuid());
}

////////////////////////////////////////////////////////////////////////////////
// Kudu Tablet Server List
////////////////////////////////////////////////////////////////////////////////

void kudu_tablet_server_list_destroy(kudu_tablet_server_list* list) {
    for (auto tserver : list->list_) {
        delete tserver;
    }
    delete list;
}

size_t kudu_tablet_server_list_size(const kudu_tablet_server_list* list) {
  return list->list_.size();
}

const kudu_tablet_server* kudu_tablet_server_list_get(const kudu_tablet_server_list* list,
                                                      size_t idx) {
  return reinterpret_cast<const kudu_tablet_server*>(list->list_[idx]);
}

////////////////////////////////////////////////////////////////////////////////
// Kudu Schema
////////////////////////////////////////////////////////////////////////////////

void kudu_schema_destroy(kudu_schema* schema) {
  delete schema;
}

size_t kudu_schema_num_columns(const kudu_schema* schema) {
  return schema->schema_.num_columns();
}

size_t kudu_schema_num_key_columns(const kudu_schema* schema) {
  vector<int> v;
  schema->schema_.GetPrimaryKeyColumnIndexes(&v);
  return v.size();
}

kudu_column_schema* kudu_schema_column(const kudu_schema* schema, size_t idx) {
  return new kudu_column_schema(schema->schema_.Column(idx));
}

kudu_status* kudu_schema_find_column(const kudu_schema* schema, kudu_slice column_name, size_t* idx) {
  return Status::into_kudu_status(schema->schema_.FindColumn(slice_to_Slice(column_name), idx));
}

kudu_partial_row* kudu_schema_new_row(const kudu_schema* schema) {
  return reinterpret_cast<kudu_partial_row*>(schema->schema_.NewRow());
}

////////////////////////////////////////////////////////////////////////////////
// Kudu Column Schema
////////////////////////////////////////////////////////////////////////////////

void kudu_column_schema_destroy(kudu_column_schema* column) {
  delete column;
}

kudu_slice kudu_column_schema_name(const kudu_column_schema* column) {
  return string_to_slice(column->column_.name());
}

int32_t kudu_column_schema_is_nullable(const kudu_column_schema* column) {
  return column->column_.is_nullable();
}

kudu_data_type kudu_column_schema_data_type(const kudu_column_schema* column) {
  return static_cast<kudu_data_type>(column->column_.type());
}

kudu_encoding_type kudu_column_schema_encoding_type(const kudu_column_schema* column) {
  return static_cast<kudu_encoding_type>(column->column_.encoding_type());
}

kudu_compression_type kudu_column_schema_compression_type(const kudu_column_schema* column) {
  return static_cast<kudu_compression_type>(column->column_.compression_type());
}

////////////////////////////////////////////////////////////////////////////////
// Kudu Schema Builder
////////////////////////////////////////////////////////////////////////////////

kudu_schema_builder* kudu_schema_builder_create() {
  return new kudu_schema_builder;
}

void kudu_schema_builder_destroy(kudu_schema_builder* builder) {
  delete builder;
}

kudu_column_schema_builder* kudu_schema_builder_add_column(kudu_schema_builder* builder,
                                                           kudu_slice name) {
  return reinterpret_cast<kudu_column_schema_builder*>(
      builder->builder_.AddColumn(slice_to_string(name)));
}

void kudu_schema_builder_set_primary_key_columns(kudu_schema_builder* builder,
                                                 kudu_slice_list column_names) {
  builder->builder_.SetPrimaryKey(slice_list_to_vector(column_names));
}

kudu_status* kudu_schema_builder_build(kudu_schema_builder* builder, kudu_schema**const schema) {
  unique_ptr<kudu_schema> s(new kudu_schema);
  RETURN_NOT_OK_C(builder->builder_.Build(&s->schema_));
  *schema = s.release();
  return nullptr;
}

////////////////////////////////////////////////////////////////////////////////
// Kudu Column Schema Builder
////////////////////////////////////////////////////////////////////////////////

void kudu_column_schema_builder_data_type(kudu_column_schema_builder* builder,
                                          kudu_data_type data_type) {
  to_internal(builder)->Type(static_cast<KuduColumnSchema::DataType>(data_type));
}

void kudu_column_schema_builder_encoding_type(kudu_column_schema_builder* builder,
                                              kudu_encoding_type encoding_type) {
  to_internal(builder)->Encoding(
      static_cast<KuduColumnStorageAttributes::EncodingType>(encoding_type));
}

void kudu_column_schema_builder_compression_type(kudu_column_schema_builder* builder,
                                                 kudu_compression_type compression_type) {
  to_internal(builder)->Compression(
      static_cast<KuduColumnStorageAttributes::CompressionType>(compression_type));
}

void kudu_column_schema_builder_block_size(kudu_column_schema_builder* builder,
                                           int32_t block_size) {
  to_internal(builder)->BlockSize(block_size);
}

void kudu_column_schema_builder_nullable(kudu_column_schema_builder* builder, int32_t nullable) {
  if (nullable) {
    to_internal(builder)->Nullable();
  } else {
    to_internal(builder)->NotNull();
  }
}

////////////////////////////////////////////////////////////////////////////////
// Kudu Client
////////////////////////////////////////////////////////////////////////////////

void kudu_client_destroy(kudu_client* client) {
  delete client;
}

kudu_table_creator* kudu_client_new_table_creator(kudu_client* client) {
  return reinterpret_cast<kudu_table_creator*>(client->client_->NewTableCreator());
}

kudu_status* kudu_client_delete_table(kudu_client* client, kudu_slice table_name) {
  return Status::into_kudu_status(client->client_->DeleteTable(slice_to_string(table_name)));
}

kudu_status* kudu_client_get_table_schema(kudu_client* client,
                                          kudu_slice table_name,
                                          kudu_schema**const schema) {
  unique_ptr<kudu_schema> s(new kudu_schema);
  RETURN_NOT_OK_C(client->client_->GetTableSchema(slice_to_string(table_name), &s.get()->schema_));
  *schema = s.release();
  return nullptr;
}

kudu_status* kudu_client_list_tables(kudu_client* client, kudu_slice filter, kudu_table_list**const tables) {
  unique_ptr<kudu_table_list> list(new kudu_table_list);
  RETURN_NOT_OK_C(client->client_->ListTables(&list->list_, slice_to_string(filter)));
  *tables = list.release();
  return nullptr;
}

kudu_status* kudu_client_list_tablet_servers(kudu_client* client,
                                             kudu_tablet_server_list**const tservers) {
    unique_ptr<kudu_tablet_server_list> list(new kudu_tablet_server_list);
    RETURN_NOT_OK_C(client->client_->ListTabletServers(&list->list_));
    *tservers = list.release();
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////
// Kudu Table Creator
////////////////////////////////////////////////////////////////////////////////

void kudu_table_creator_destroy(kudu_table_creator* creator) {
  delete to_internal(creator);
}

void kudu_table_creator_table_name(kudu_table_creator* creator,
                                   kudu_slice table_name) {
  to_internal(creator)->table_name(slice_to_string(table_name));
}

void kudu_table_creator_schema(kudu_table_creator* creator, const kudu_schema* schema) {
  to_internal(creator)->schema(&schema->schema_);
}

void kudu_table_creator_add_hash_partitions(kudu_table_creator* creator,
                                            kudu_slice_list columns,
                                            int32_t num_buckets,
                                            int32_t seed) {
  to_internal(creator)->add_hash_partitions(slice_list_to_vector(columns), num_buckets, seed);
}

void kudu_table_creator_set_range_partition_columns(kudu_table_creator* creator,
                                                    kudu_slice_list columns) {
  to_internal(creator)->set_range_partition_columns(slice_list_to_vector(columns));
}

void kudu_table_creator_add_split_row(kudu_table_creator* creator, kudu_partial_row* split_row) {
  to_internal(creator)->add_split_row(to_internal(split_row));
}

void kudu_table_creator_num_replicas(kudu_table_creator* creator, int32_t num_replicas) {
  to_internal(creator)->num_replicas(num_replicas);
}

void kudu_table_creator_timeout(kudu_table_creator* creator, int64_t timeout_ms) {
  to_internal(creator)->timeout(MonoDelta::FromMilliseconds(timeout_ms));
}

void kudu_table_creator_wait(kudu_table_creator* creator, int32_t wait) {
  to_internal(creator)->wait(wait);
}

kudu_status* kudu_table_creator_create(kudu_table_creator* creator) {
  return Status::into_kudu_status(to_internal(creator)->Create());
}


////////////////////////////////////////////////////////////////////////////////
// Kudu Partial Row
////////////////////////////////////////////////////////////////////////////////

void kudu_partial_row_destroy(kudu_partial_row* row) {
  delete to_internal(row);
}

int32_t/*bool*/ kudu_partial_row_is_primary_key_set(const kudu_partial_row* row) {
  return to_internal(row)->IsKeySet();
}

int32_t/*bool*/ kudu_partial_row_all_columns_set(const kudu_partial_row* row) {
  return to_internal(row)->IsKeySet();
}

kudu_status* kudu_partial_row_set_bool_by_name(kudu_partial_row* row,
                                               kudu_slice column_name,
                                               int32_t/*bool*/ val) {
  return Status::into_kudu_status(to_internal(row)->SetBool(slice_to_Slice(column_name), val));
}
kudu_status* kudu_partial_row_set_int8_by_name(kudu_partial_row* row,
                                               kudu_slice column_name,
                                               int8_t val) {
  return Status::into_kudu_status(to_internal(row)->SetInt8(slice_to_Slice(column_name), val));
}
kudu_status* kudu_partial_row_set_int16_by_name(kudu_partial_row* row,
                                                kudu_slice column_name,
                                                int16_t val) {
  return Status::into_kudu_status(to_internal(row)->SetInt16(slice_to_Slice(column_name), val));
}
kudu_status* kudu_partial_row_set_int32_by_name(kudu_partial_row* row,
                                                kudu_slice column_name,
                                                int32_t val) {
  return Status::into_kudu_status(to_internal(row)->SetInt32(slice_to_Slice(column_name), val));
}
kudu_status* kudu_partial_row_set_int64_by_name(kudu_partial_row* row,
                                                kudu_slice column_name,
                                                int64_t val) {
  return Status::into_kudu_status(to_internal(row)->SetInt64(slice_to_Slice(column_name), val));
}
kudu_status* kudu_partial_row_set_timestamp_by_name(kudu_partial_row* row,
                                                    kudu_slice column_name,
                                                    int64_t val) {
  return Status::into_kudu_status(to_internal(row)->SetTimestamp(slice_to_Slice(column_name), val));
}
kudu_status* kudu_partial_row_set_float_by_name(kudu_partial_row* row,
                                                kudu_slice column_name,
                                                float val) {
  return Status::into_kudu_status(to_internal(row)->SetFloat(slice_to_Slice(column_name), val));
}
kudu_status* kudu_partial_row_set_double_by_name(kudu_partial_row* row,
                                                 kudu_slice column_name,
                                                 double val) {
  return Status::into_kudu_status(to_internal(row)->SetDouble(slice_to_Slice(column_name), val));
}
kudu_status* kudu_partial_row_set_string_by_name(kudu_partial_row* row,
                                                 kudu_slice column_name,
                                                 kudu_slice val) {
  return Status::into_kudu_status(to_internal(row)->SetString(slice_to_Slice(column_name),
                                                      slice_to_Slice(val)));
}
kudu_status* kudu_partial_row_set_string_copy_by_name(kudu_partial_row* row,
                                                      kudu_slice column_name,
                                                      kudu_slice val) {
  return Status::into_kudu_status(to_internal(row)->SetStringCopy(slice_to_Slice(column_name),
                                                                  slice_to_Slice(val)));
}
kudu_status* kudu_partial_row_set_binary_by_name(kudu_partial_row* row,
                                                 kudu_slice column_name,
                                                 kudu_slice val) {
  return Status::into_kudu_status(to_internal(row)->SetBinary(slice_to_Slice(column_name),
                                                              slice_to_Slice(val)));
}
kudu_status* kudu_partial_row_set_binary_copy_by_name(kudu_partial_row* row,
                                                      kudu_slice column_name,
                                                      kudu_slice val) {
  return Status::into_kudu_status(to_internal(row)->SetBinaryCopy(slice_to_Slice(column_name),
                                                                  slice_to_Slice(val)));
}
kudu_status* kudu_partial_row_set_null_by_name(kudu_partial_row* row, kudu_slice column_name) {
  return Status::into_kudu_status(to_internal(row)->SetNull(slice_to_Slice(column_name)));
}
kudu_status* kudu_partial_row_unset_by_name(kudu_partial_row* row, kudu_slice column_name) {
  return Status::into_kudu_status(to_internal(row)->Unset(slice_to_Slice(column_name)));
}

kudu_status* kudu_partial_row_set_bool(kudu_partial_row* row,
                                       size_t column_idx,
                                       int32_t/*bool*/ val) {
  return Status::into_kudu_status(to_internal(row)->SetBool(column_idx, val));
}
kudu_status* kudu_partial_row_set_int8(kudu_partial_row* row,
                                       size_t column_idx,
                                       int8_t val) {
  return Status::into_kudu_status(to_internal(row)->SetInt8(column_idx, val));
}
kudu_status* kudu_partial_row_set_int16(kudu_partial_row* row,
                                        size_t column_idx,
                                        int16_t val) {
  return Status::into_kudu_status(to_internal(row)->SetInt16(column_idx, val));
}
kudu_status* kudu_partial_row_set_int32(kudu_partial_row* row,
                                        size_t column_idx,
                                        int32_t val) {
  return Status::into_kudu_status(to_internal(row)->SetInt32(column_idx, val));
}
kudu_status* kudu_partial_row_set_int64(kudu_partial_row* row,
                                        size_t column_idx,
                                        int64_t val) {
  return Status::into_kudu_status(to_internal(row)->SetInt64(column_idx, val));
}
kudu_status* kudu_partial_row_set_timestamp(kudu_partial_row* row,
                                            size_t column_idx,
                                            int64_t val) {
  return Status::into_kudu_status(to_internal(row)->SetTimestamp(column_idx, val));
}
kudu_status* kudu_partial_row_set_float(kudu_partial_row* row,
                                        size_t column_idx,
                                        float val) {
  return Status::into_kudu_status(to_internal(row)->SetFloat(column_idx, val));
}
kudu_status* kudu_partial_row_set_double(kudu_partial_row* row,
                                         size_t column_idx,
                                         double val) {
  return Status::into_kudu_status(to_internal(row)->SetDouble(column_idx, val));
}
kudu_status* kudu_partial_row_set_string(kudu_partial_row* row,
                                         size_t column_idx,
                                         kudu_slice val) {
  return Status::into_kudu_status(to_internal(row)->SetString(column_idx, slice_to_Slice(val)));
}
kudu_status* kudu_partial_row_set_string_copy(kudu_partial_row* row,
                                              size_t column_idx,
                                              kudu_slice val) {
  return Status::into_kudu_status(to_internal(row)->SetStringCopy(column_idx, slice_to_Slice(val)));
}
kudu_status* kudu_partial_row_set_binary(kudu_partial_row* row,
                                         size_t column_idx,
                                         kudu_slice val) {
  return Status::into_kudu_status(to_internal(row)->SetBinary(column_idx, slice_to_Slice(val)));
}
kudu_status* kudu_partial_row_set_binary_copy(kudu_partial_row* row,
                                              size_t column_idx,
                                              kudu_slice val) {
  return Status::into_kudu_status(to_internal(row)->SetBinaryCopy(column_idx, slice_to_Slice(val)));
}
kudu_status* kudu_partial_row_set_null(kudu_partial_row* row, size_t column_idx) {
  return Status::into_kudu_status(to_internal(row)->SetNull(column_idx));
}
kudu_status* kudu_partial_row_unset(kudu_partial_row* row, size_t column_idx) {
  return Status::into_kudu_status(to_internal(row)->Unset(column_idx));
}

kudu_status* kudu_partial_row_get_bool_by_name(const kudu_partial_row* row,
                                               kudu_slice column_name,
                                               int32_t/*bool*/* val) {
  bool b;
  RETURN_NOT_OK_C(to_internal(row)->GetBool(slice_to_Slice(column_name), &b));
  *val = b;
  return nullptr;
}
kudu_status* kudu_partial_row_get_int8_by_name(const kudu_partial_row* row,
                                               kudu_slice column_name,
                                               int8_t* val) {
  return Status::into_kudu_status(to_internal(row)->GetInt8(slice_to_Slice(column_name), val));
}
kudu_status* kudu_partial_row_get_int16_by_name(const kudu_partial_row* row,
                                                kudu_slice column_name,
                                                int16_t* val) {
  return Status::into_kudu_status(to_internal(row)->GetInt16(slice_to_Slice(column_name), val));
}
kudu_status* kudu_partial_row_get_int32_by_name(const kudu_partial_row* row,
                                                kudu_slice column_name,
                                                int32_t* val) {
  return Status::into_kudu_status(to_internal(row)->GetInt32(slice_to_Slice(column_name), val));
}
kudu_status* kudu_partial_row_get_int64_by_name(const kudu_partial_row* row,
                                                kudu_slice column_name,
                                                int64_t* val) {
  return Status::into_kudu_status(to_internal(row)->GetInt64(slice_to_Slice(column_name), val));
}
kudu_status* kudu_partial_row_get_timestamp_by_name(const kudu_partial_row* row,
                                                    kudu_slice column_name,
                                                    int64_t* val) {
  return Status::into_kudu_status(to_internal(row)->GetTimestamp(slice_to_Slice(column_name), val));
}
kudu_status* kudu_partial_row_get_float_by_name(const kudu_partial_row* row,
                                                kudu_slice column_name,
                                                float* val) {
  return Status::into_kudu_status(to_internal(row)->GetFloat(slice_to_Slice(column_name), val));
}
kudu_status* kudu_partial_row_get_double_by_name(const kudu_partial_row* row,
                                                 kudu_slice column_name,
                                                 double* val) {
  return Status::into_kudu_status(to_internal(row)->GetDouble(slice_to_Slice(column_name), val));
}
kudu_status* kudu_partial_row_get_string_by_name(const kudu_partial_row* row,
                                                 kudu_slice column_name,
                                                 kudu_slice* val) {
  Slice s;
  RETURN_NOT_OK_C(to_internal(row)->GetString(slice_to_Slice(column_name), &s));
  val->data = s.data();
  val->len = s.size();
  return nullptr;
}
kudu_status* kudu_partial_row_get_binary_by_name(const kudu_partial_row* row,
                                                 kudu_slice column_name,
                                                 kudu_slice* val) {
  Slice s;
  RETURN_NOT_OK_C(to_internal(row)->GetBinary(slice_to_Slice(column_name), &s));
  val->data = s.data();
  val->len = s.size();
  return nullptr;
}
int32_t/*bool*/ kudu_partial_row_is_null_by_name(const kudu_partial_row* row, kudu_slice column_name) {
  return to_internal(row)->IsNull(slice_to_Slice(column_name));
}
int32_t/*bool*/ kudu_partial_row_is_set_by_name(const kudu_partial_row* row, kudu_slice column_name) {
  return to_internal(row)->IsColumnSet(slice_to_Slice(column_name));
}

kudu_status* kudu_partial_row_get_bool(const kudu_partial_row* row,
                                       size_t column_idx,
                                       int32_t/*bool*/* val) {
  bool b;
  RETURN_NOT_OK_C(to_internal(row)->GetBool(column_idx, &b));
  *val = b;
  return nullptr;
}
kudu_status* kudu_partial_row_get_int8(const kudu_partial_row* row,
                                       size_t column_idx,
                                       int8_t* val) {
  return Status::into_kudu_status(to_internal(row)->GetInt8(column_idx, val));
}
kudu_status* kudu_partial_row_get_int16(const kudu_partial_row* row,
                                        size_t column_idx,
                                        int16_t* val) {
  return Status::into_kudu_status(to_internal(row)->GetInt16(column_idx, val));
}
kudu_status* kudu_partial_row_get_int32(const kudu_partial_row* row,
                                        size_t column_idx,
                                        int32_t* val) {
  return Status::into_kudu_status(to_internal(row)->GetInt32(column_idx, val));
}
kudu_status* kudu_partial_row_get_int64(const kudu_partial_row* row,
                                        size_t column_idx,
                                        int64_t* val) {
  return Status::into_kudu_status(to_internal(row)->GetInt64(column_idx, val));
}
kudu_status* kudu_partial_row_get_timestamp(const kudu_partial_row* row,
                                            size_t column_idx,
                                            int64_t* val) {
  return Status::into_kudu_status(to_internal(row)->GetTimestamp(column_idx, val));
}
kudu_status* kudu_partial_row_get_float(const kudu_partial_row* row,
                                        size_t column_idx,
                                        float* val) {
  return Status::into_kudu_status(to_internal(row)->GetFloat(column_idx, val));
}
kudu_status* kudu_partial_row_get_double(const kudu_partial_row* row,
                                         size_t column_idx,
                                         double* val) {
  return Status::into_kudu_status(to_internal(row)->GetDouble(column_idx, val));
}
kudu_status* kudu_partial_row_get_string(const kudu_partial_row* row,
                                         size_t column_idx,
                                         kudu_slice* val) {
  Slice s;
  RETURN_NOT_OK_C(to_internal(row)->GetString(column_idx, &s));
  val->data = s.data();
  val->len = s.size();
  return nullptr;
}
kudu_status* kudu_partial_row_get_binary(const kudu_partial_row* row,
                                         size_t column_idx,
                                         kudu_slice* val) {
  Slice s;
  RETURN_NOT_OK_C(to_internal(row)->GetBinary(column_idx, &s));
  val->data = s.data();
  val->len = s.size();
  return nullptr;
}
int32_t/*bool*/ kudu_partial_row_is_null(const kudu_partial_row* row, size_t column_idx) {
  return to_internal(row)->IsNull(column_idx);
}
int32_t/*bool*/ kudu_partial_row_is_set(const kudu_partial_row* row, size_t column_idx) {
  return to_internal(row)->IsColumnSet(column_idx);
}

} // extern "C"
