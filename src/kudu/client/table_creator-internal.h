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
#ifndef KUDU_CLIENT_TABLE_CREATOR_INTERNAL_H
#define KUDU_CLIENT_TABLE_CREATOR_INTERNAL_H

#include <string>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/common/common.pb.h"

namespace kudu {
namespace client {
namespace internal {

class HashPartitionCreator;

class TableCreator {
 public:
  explicit TableCreator(internal::Client* client);
  ~TableCreator();

  void set_table_name(std::string name) {
    table_name_ = std::move(name);
  }

  void set_schema(const Schema* schema) {
    schema_ = schema;
  }

  HashPartitionCreator add_hash_partition();

  void add_range_partition_column(std::string name);

  void clear_range_partition_columns();

  void set_num_replicas(int32_t num_replicas) {
    num_replicas_ = num_replicas;
  }

  void set_timeout(MonoDelta timeout) {
    timeout_ = std::move(timeout);
  }

  void set_wait(bool wait) {
    wait_ = wait;
  }

  void add_range_partition_split(const KuduPartialRow* row) {
    range_partition_splits_.push_back(row);
  }

  void clear_range_partition_splits();

  // Creates the table.
  Status Create();

 private:

  internal::Client* client_;

  std::string table_name_;

  const Schema* schema_;

  std::vector<const KuduPartialRow*> range_partition_splits_;

  PartitionSchemaPB partition_schema_;

  int32_t num_replicas_;

  MonoDelta timeout_;

  bool wait_;

  DISALLOW_COPY_AND_ASSIGN(TableCreator);
};

class HashPartitionCreator {
 public:

  ~HashPartitionCreator() = default;

  void add_column(std::string name) {
    schema_->add_columns()->set_name(std::move(name));
  }

  void clear_columns() {
    schema_->clear_columns();
  }

  void set_num_hash_buckets(int32_t num_buckets) {
    schema_->set_num_buckets(num_buckets);
  }

  void set_seed(int32_t seed) {
    schema_->set_seed(seed);
  }

 private:
  friend class TableCreator;

  HashPartitionCreator(PartitionSchemaPB::HashBucketSchemaPB* schema)
      : schema_(schema) {
  }

  PartitionSchemaPB::HashBucketSchemaPB* schema_;
};

} // namespace internal
} // namespace client
} // namespace kudu

#endif
