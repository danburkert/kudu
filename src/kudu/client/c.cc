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
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

using std::memcpy;
using std::move;
using std::string;
using std::unique_ptr;
using std::vector;

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduSchema;
using kudu::client::sp::shared_ptr;
using kudu::MonoDelta;
using kudu::Status;

extern "C" {

struct kudu_client_builder { KuduClientBuilder builder_; };
struct kudu_client { shared_ptr<KuduClient> client_; };
struct kudu_schema { KuduSchema schema_; };
struct kudu_table_list { vector<string> list_; };

struct kudu_column_schema {
  kudu_column_schema(KuduColumnSchema column) : column_(move(column)) {}
  KuduColumnSchema column_;
};

////////////////////////////////////////////////////////////////////////////////
// Kudu Status
//
// kudu_status is an alias to a const char* with the same internal format as
// Status::state_.
////////////////////////////////////////////////////////////////////////////////

void kudu_status_destroy(const kudu_status* status) {
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
  builder->builder_.add_master_server_addr(string(reinterpret_cast<const char*>(addr.data),
                                                  addr.len));
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

const kudu_status* kudu_client_builder_build(kudu_client_builder* builder,
                                             kudu_client** client) {
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
  const string& name = list->list_[index];
  const uint8_t* data = reinterpret_cast<const uint8_t*>(name.data());
  return kudu_slice { .data = data, .len = name.size() };
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

////////////////////////////////////////////////////////////////////////////////
// Kudu Column Schema
////////////////////////////////////////////////////////////////////////////////

void kudu_column_schema_destroy(kudu_column_schema* column) {
  delete column;
}

kudu_slice kudu_column_schema_name(const kudu_column_schema* column) {
  const string& name = column->column_.name();
  const uint8_t* data = reinterpret_cast<const uint8_t*>(name.data());
  return kudu_slice { .data = data, .len = name.size() };
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
// Kudu Client
////////////////////////////////////////////////////////////////////////////////

void kudu_client_destroy(kudu_client* client) {
  delete client;
}

// Returns the tables.
const kudu_status* kudu_client_list_tables(const kudu_client* client,
                                           kudu_table_list** tables) {
  unique_ptr<kudu_table_list> list(new kudu_table_list);
  RETURN_NOT_OK_C(client->client_->ListTables(&list->list_));
  *tables = list.release();
  return nullptr;
}

const kudu_status* kudu_client_table_schema(const kudu_client* client,
                                            kudu_slice table_name,
                                            kudu_schema** schema) {
  unique_ptr<kudu_schema> s(new kudu_schema);
  RETURN_NOT_OK_C(client->client_->GetTableSchema(
        string(reinterpret_cast<const char*>(table_name.data), table_name.len),
        &s.get()->schema_));
  *schema = s.release();
  return nullptr;
}
}
