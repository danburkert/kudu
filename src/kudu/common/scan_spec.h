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
#ifndef KUDU_COMMON_SCAN_SPEC_H
#define KUDU_COMMON_SCAN_SPEC_H

#include <string>
#include <unordered_map>

#include "kudu/common/schema.h"
#include "kudu/common/column_predicate.h"
#include "kudu/common/encoded_key.h"

namespace kudu {

class AutoReleasePool;
class Arena;

class ScanSpec {
 public:
  ScanSpec()
    : predicates_(),
      lower_bound_key_(nullptr),
      exclusive_upper_bound_key_(nullptr),
      lower_bound_partition_key_(),
      exclusive_upper_bound_partition_key_(),
      cache_blocks_(true) {
  }

  // Add a predicate on the column.
  //
  // The new predicate is merged into the existing predicate for the column.
  void AddPredicate(ColumnPredicate pred);

  // Remove the predicate for the column.
  void RemovePredicate(const std::string& column_name);

  // Removes all column predicates.
  void RemovePredicates();

  // Returns true if the result set is known to be empty.
  bool CanShortCircuit() const;

  // Optimizes the scan by unifying the lower and upper bound constraints and
  // the column predicates.
  //
  // If remove_pushed_predicates is true, then column predicates that are pushed
  // into the upper or lower primary key bounds are removed.
  //
  // Idempotent.
  void OptimizeScan(const Schema& schema,
                    Arena* arena,
                    AutoReleasePool* pool,
                    bool remove_pushed_predicates);

  // Set the lower bound (inclusive) primary key for the scan.
  // Does not take ownership of 'key', which must remain valid.
  // If called multiple times, the most restrictive key will be used.
  void SetLowerBoundKey(const EncodedKey* key);

  // Set the upper bound (exclusive) primary key for the scan.
  // Does not take ownership of 'key', which must remain valid.
  // If called multiple times, the most restrictive key will be used.
  void SetExclusiveUpperBoundKey(const EncodedKey* key);

  // Sets the lower bound (inclusive) partition key for the scan.
  //
  // The scan spec makes a copy of 'slice'; the caller may free it afterward.
  //
  // Only used in the client.
  void SetLowerBoundPartitionKey(const Slice& slice);

  // Sets the upper bound (exclusive) partition key for the scan.
  //
  // The scan spec makes a copy of 'slice'; the caller may free it afterward.
  //
  // Only used in the client.
  void SetExclusiveUpperBoundPartitionKey(const Slice& slice);

  // Returns the scan predicates.
  const std::unordered_map<std::string, ColumnPredicate>& predicates() const {
    return predicates_;
  }

  // Return a pointer to the list of predicates in this scan spec.
  //
  // Callers may use this during predicate pushdown to remove predicates
  // from their caller if they're able to apply them lower down the
  // iterator tree.
  vector<ColumnRangePredicate> *mutable_predicates() {
    return &predicates_;
  }

  const EncodedKey* lower_bound_key() const {
    return lower_bound_key_;
  }

  const EncodedKey* exclusive_upper_bound_key() const {
    return exclusive_upper_bound_key_;
  }

  const string& lower_bound_partition_key() const {
    return lower_bound_partition_key_;
  }
  const string& exclusive_upper_bound_partition_key() const {
    return exclusive_upper_bound_partition_key_;
  }

  bool cache_blocks() const {
    return cache_blocks_;
  }

  void set_cache_blocks(bool cache_blocks) {
    cache_blocks_ = cache_blocks;
  }

  std::string ToString(const Schema& s) const;

 private:

  // Lift implicit predicates specified as part of the lower and upper bound
  // primary key constraints into the simplified predicate bounds.
  //
  // When the lower and exclusive upper bound primary keys have a prefix of
  // equal components, the components can be lifted into an equality predicate
  // over their associated column. Optionally, a single (pair) of range
  // predicates can be lifted from the key component following the prefix of
  // equal components.
  void LiftPrimaryKeyBounds(const Schema& schema, Arena* arena);

  // Encode the column predicates into lower and upper primary key bounds, and
  // replace the existing bounds if the new bounds are more constrained.
  //
  // If remove_pushed_predicates is true, then the predicates in the primary key
  // bound will be removed if the bound is replaced.
  void PushPredicatesIntoPrimaryKeyBounds(const Schema& schema,
                                          Arena* arena,
                                          AutoReleasePool* pool,
                                          bool remove_pushed_predicates);

  std::unordered_map<std::string, ColumnPredicate> predicates_;
  const EncodedKey* lower_bound_key_;
  const EncodedKey* exclusive_upper_bound_key_;
  std::string lower_bound_partition_key_;
  std::string exclusive_upper_bound_partition_key_;
  bool cache_blocks_;
};

} // namespace kudu

#endif
