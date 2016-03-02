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
#include <tuple>
#include <vector>

#include "kudu/common/partition.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"

namespace kudu {

class Partition;
class PartitionSchema;
class ScanSpec;
class Schema;

class PartitionPruner {
 public:

  // Initializes the partition pruner for a new scan. The scan spec should
  // already be optimized.
  void Init(const Schema& schema,
            const PartitionSchema& partition_schema,
            const ScanSpec& scan_spec);

  // Returns whether there are more partition key ranges to scan.
  bool HasMorePartitions() const;

  // Returns the inclusive lower bound partition key of the next tablet to scan.
  const std::string& NextPartitionKey() const;

  // Removes partition key ranges through the exclusive upper bound.
  void RemovePartitionKeyRange(const std::string& upper_bound);

  // Returns true if the provided partition should be pruned.
  //
  // Used for testing.
  bool ShouldPrune(const Partition& partition) const;

  std::string ToString(const Schema& schema, const PartitionSchema& partition_schema) const;

 private:
  // The reverse sorted set of partition key ranges. Each range has an inclusive
  // lower and exclusive upper bound.
  std::vector<std::tuple<std::string, std::string>> partition_key_ranges_;
};

} // namespace kudu
