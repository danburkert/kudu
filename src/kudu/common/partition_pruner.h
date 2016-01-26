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
#include <vector>

namespace kudu {

class Partition;
class PartitionSchema;
class ScanSpec;
class Schema;

class PartitionPruner {
 public:

  PartitionPruner(const Schema& schema,
                  const PartitionSchema& partition_schema,
                  const ScanSpec& scan_spec);

  // Returns true if the provided partition should be pruned.
  bool should_prune(const Partition& partition) const;

  // Functor method which calls should_prune.
  bool operator()(const Partition& partition) const {
    return this->should_prune(partition);
  }

 private:

  // The first (inclusive) range key in the scan.
  std::string range_key_start_;

  // The final (exclusive) range key in the scan.
  std::string range_key_end_;

  // For each hash component in the table, this vector holds the value of the
  // hash bucket if the scan is constrained to a single hash bucket, or -1.
  std::vector<int32_t> hash_buckets_;
};

} // namespace kudu
