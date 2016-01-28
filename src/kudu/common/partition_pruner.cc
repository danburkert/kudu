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

#include "kudu/common/partition_pruner.h"

#include "kudu/common/partition.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/util/status.h"

namespace kudu {

PartitionPruner::PartitionPruner(const Schema& schema,
                                 const PartitionSchema& partition_schema,
                                 const ScanSpec& spec)
    : range_key_start_(),
      range_key_end_(),
      hash_buckets_() {
  hash_buckets_.reserve(partition_schema.hash_bucket_schemas_.size());

  if (!spec.predicates().empty()) {

    // Map of column ID to predicate
    unordered_map<ColumnId, const ColumnRangePredicate*> predicates;
    for (const ColumnRangePredicate& predicate : spec.predicates()) {
      const string& column_name = predicate.column().name();
      int32_t column_idx = schema.find_column(column_name);
      if (column_idx == Schema::kColumnNotFound) {
        LOG(FATAL) << "Scan spec contained an unknown column: " << column_name;
      }
      // TODO: check for emplace fail due to multiple predicates on same column?
      predicates.emplace(schema.column_id(column_idx), predicate);
    }

    vector<int32_t> buckets;

    // First optimization:
    //
    // If the table is hash partitioned, then we can walk the hash partitions in
    // order and check whether the scan spec specifies an equality relation on
    // the columns in the hash component. If so, the partition key range can be
    // reduced to cover only that hash bucket.
    for (const auto& hash_bucket_schema : partition_schema.hash_bucket_schemas_) {
      string encoded_value;
      bool cont = true;
      for (vector<ColumnId>::const_iterator column_id = hash_bucket_schema.column_ids.begin();
           cont && column_id != hash_bucket_schema.column_ids.end(); column_id++) {
        const ColumnRangePredicate** predicate = FindOrNull(predicates, *column_id);
        if (predicate != nullptr && (*predicate)->range().IsEquality()) {

        } else {
          buckets.push_back(-1);
          break;
        }
      }
    }
  }
}
} // namespace kudu
