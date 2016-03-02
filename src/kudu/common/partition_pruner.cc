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

#include <algorithm>
#include <boost/optional.hpp>
#include <cstring>
#include <memory>
#include <numeric>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "kudu/common/key_encoder.h"
#include "kudu/common/key_util.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"

using boost::optional;
using std::get;
using std::lower_bound;
using std::make_tuple;
using std::memcpy;
using std::min;
using std::move;
using std::string;
using std::tuple;
using std::unique_ptr;
using std::unordered_map;
using std::vector;

namespace kudu {
namespace {

// Returns true if the partition schema's range columns are a prefix of the
// primary key columns.
bool AreRangeColumnsPrefixOfPrimaryKey(const Schema& schema,
                                       const vector<ColumnId>& range_columns) {
  for (int32_t col_idx = 0; col_idx < range_columns.size(); col_idx++) {
    if (schema.column_id(col_idx) != range_columns[col_idx]) {
      return false;
    }
  }
  return true;
}

// Translates the scan primary key bounds into range keys. This should only be
// used when the range columns are a prefix of the primary key columns.
void EncodeRangeKeysFromPrimaryKeyBounds(const Schema& schema,
                                         const ScanSpec& scan_spec,
                                         size_t num_range_columns,
                                         string* range_key_start,
                                         string* range_key_end) {
  // Don't bother if there are no lower/upper PK bounds
  if (scan_spec.lower_bound_key() != nullptr || scan_spec.exclusive_upper_bound_key() != nullptr) {
    if (num_range_columns == schema.num_key_columns()) {
      // The range columns are the primary key columns, so the range key is
      // the primary key.
      if (scan_spec.lower_bound_key() != nullptr) {
        *range_key_start = scan_spec.lower_bound_key()->encoded_key().ToString();
      }
      if (scan_spec.exclusive_upper_bound_key() != nullptr) {
        *range_key_end = scan_spec.exclusive_upper_bound_key()->encoded_key().ToString();
      }
    } else {
      // The range columns are a prefix subset of the primary key columns. Copy
      // the column values over to a row, and then encode the row as a range
      // key.

      vector<int32_t> col_idxs(num_range_columns);
      std::iota(col_idxs.begin(), col_idxs.end(), 0);

      unique_ptr<uint8_t[]> buf(new uint8_t[schema.key_byte_size()]);
      ContiguousRow row(&schema, buf.get());

      if (scan_spec.lower_bound_key() != nullptr) {
        for (int32_t idx : col_idxs) {
          memcpy(row.mutable_cell_ptr(idx),
                 scan_spec.lower_bound_key()->raw_keys()[idx],
                 schema.column(idx).type_info()->size());
        }
        key_util::EncodeKey(col_idxs, row, range_key_start);
      }

      if (scan_spec.exclusive_upper_bound_key() != nullptr) {
        for (int32_t idx : col_idxs) {
          memcpy(row.mutable_cell_ptr(idx),
                 scan_spec.exclusive_upper_bound_key()->raw_keys()[idx],
                 schema.column(idx).type_info()->size());
        }
        key_util::EncodeKey(col_idxs, row, range_key_end);
      }
    }
  }
}

// Push the scan predicates into the range keys.
void EncodeRangeKeysFromPredicates(const Schema& schema,
                                   const unordered_map<string, ColumnPredicate>& predicates,
                                   const vector<ColumnId>& range_columns,
                                   string* range_key_start,
                                   string* range_key_end) {

  // Find the column indexes of the range columns.
  vector<int32_t> col_idxs;
  col_idxs.reserve(range_columns.size());
  for (ColumnId column : range_columns) {
    int32_t col_idx = schema.find_column_by_id(column);
    CHECK(col_idx != Schema::kColumnNotFound);
    col_idxs.push_back(col_idx);
  }

  Arena arena(max<size_t>(16, schema.key_byte_size()), 4096);
  uint8_t* buf =
    static_cast<uint8_t*>(CHECK_NOTNULL(arena.AllocateBytes(schema.key_byte_size())));
  ContiguousRow row(&schema, buf);

  if (key_util::PushLowerBoundKeyPredicates(col_idxs, predicates, &row, &arena) > 0) {
    key_util::EncodeKey(col_idxs, row, range_key_start);
  }

  if (key_util::PushUpperBoundKeyPredicates(col_idxs, predicates, &row, &arena) > 0) {
    key_util::EncodeKey(col_idxs, row, range_key_end);
  }
}
} // anonymous namespace

void PartitionPruner::Init(const Schema& schema,
                           const PartitionSchema& partition_schema,
                           const ScanSpec& scan_spec) {
  // If we can already short circuit the scan we don't need to bother with
  // partition pruning. This also allows us to assume some invariants of the
  // scan spec, such as no None predicates and that the lower bound PK < upper
  // bound PK.
  if (scan_spec.CanShortCircuit()) { return; }

  std::string range_lower_bound;
  std::string range_upper_bound;

  const vector<ColumnId>& range_columns = partition_schema.range_schema_.column_ids;
  if (!range_columns.empty()) {
    if (AreRangeColumnsPrefixOfPrimaryKey(schema, range_columns)) {
      EncodeRangeKeysFromPrimaryKeyBounds(schema,
                                          scan_spec,
                                          range_columns.size(),
                                          &range_lower_bound,
                                          &range_upper_bound);
    } else {
      EncodeRangeKeysFromPredicates(schema,
                                    scan_spec.predicates(),
                                    range_columns,
                                    &range_lower_bound,
                                    &range_upper_bound);
    }
  }

  // Construct the list of hash buckets, or none if the hash bucket is not
  // constrained.
  vector<optional<uint32_t>> hash_buckets;

  // The index of the final hash component with a constrained bucket.
  int constrained_index = -1;

  hash_buckets.reserve(partition_schema.hash_bucket_schemas_.size());
  for (int i = 0; i < partition_schema.hash_bucket_schemas_.size(); i++) {
    const auto& hash_bucket_schema = partition_schema.hash_bucket_schemas_[i];
    string encoded_columns;
    bool can_prune = true;
    for (int i = 0; i < hash_bucket_schema.column_ids.size(); i++) {
      const ColumnSchema& column = schema.column_by_id(hash_bucket_schema.column_ids[i]);
      const ColumnPredicate* predicate = FindOrNull(scan_spec.predicates(), column.name());
      if (predicate == nullptr || predicate->predicate_type() != PredicateType::Equality) {
        can_prune = false;
        break;
      }

      const KeyEncoder<string>& encoder = GetKeyEncoder<string>(column.type_info());
      encoder.Encode(predicate->raw_lower(),
                     i + 1 == hash_bucket_schema.column_ids.size(),
                     &encoded_columns);
    }
    if (can_prune) {
      hash_buckets.push_back(partition_schema.BucketForEncodedColumns(encoded_columns,
                                                                      hash_bucket_schema));
      constrained_index = i;
    } else {
      hash_buckets.push_back(boost::none);
    }
  }

  if (!range_lower_bound.empty() && !range_upper_bound.empty()) {
    constrained_index = partition_schema.hash_bucket_schemas_.size() - 1;
  }

  // Create the set of ranges.
  vector<tuple<string, string>> ranges(1);
  const KeyEncoder<string>& hash_encoder = GetKeyEncoder<string>(GetTypeInfo(UINT32));

  for (int i = 0; i <= constrained_index; i++) {
    // This is the final range key component if this is the final constrained
    // bucket, and the range upper bound is empty. In this case we need to
    // increment the bucket on the upper bound to convert from inclusive to
    // exclusive.
    bool is_last = i == constrained_index && range_upper_bound.empty();

    if (hash_buckets[i]) {
      // This hash component is constrained by equality predicates to a single
      // hash bucket.
      uint32_t bucket = *hash_buckets[i];
      uint32_t bucket_upper = is_last ? bucket + 1 : bucket;
      for (auto& range : ranges) {
        hash_encoder.Encode(&bucket, &get<0>(range));
        hash_encoder.Encode(&bucket_upper, &get<1>(range));
      }
    } else {
      const auto& hash_bucket_schema = partition_schema.hash_bucket_schemas_[i];
      // Add a range for each possible hash bucket to the set of ranges.
      vector<tuple<string, string>> new_ranges;
      for (const auto& range : ranges) {
        for (uint32_t bucket = 0; bucket < hash_bucket_schema.num_buckets; bucket++) {
          uint32_t bucket_upper = is_last ? bucket + 1 : bucket;
          string lower = get<0>(range);
          string upper = get<1>(range);
          hash_encoder.Encode(&bucket, &lower);
          hash_encoder.Encode(&bucket_upper, &upper);
          new_ranges.push_back(make_tuple(move(lower), move(upper)));
        }
      }
      ranges.swap(new_ranges);
    }
  }

  // Append the range bounds.
  for (auto& range : ranges) {
    get<0>(range).append(range_lower_bound);
    get<1>(range).append(range_upper_bound);
  }

  // Remove all ranges past the scan spec's upper bound partition key key.
  if (!scan_spec.exclusive_upper_bound_partition_key().empty()) {
    for (auto range = ranges.rbegin(); range != ranges.rend(); range++) {
      if (get<1>(*range).empty() ||
          scan_spec.exclusive_upper_bound_partition_key() < get<1>(*range)) {
        if (scan_spec.exclusive_upper_bound_partition_key() <= get<0>(*range)) {
          ranges.pop_back();
        } else {
          get<1>(*range) = scan_spec.exclusive_upper_bound_partition_key();
        }
      } else {
        break;
      }
    }
  }

  // move the partition ranges in reverse order.
  partition_key_ranges_.resize(ranges.size());
  move(ranges.rbegin(), ranges.rend(), partition_key_ranges_.begin());

  // Remove all ranges before tha scan spec's lower bound partition key.
  if (!scan_spec.lower_bound_partition_key().empty()) {
    RemovePartitionKeyRange(scan_spec.lower_bound_partition_key());
  }
}

bool PartitionPruner::HasMorePartitions() const {
  return !partition_key_ranges_.empty();
}

const string& PartitionPruner::NextPartitionKey() const {
  CHECK(HasMorePartitions());
  return get<0>(partition_key_ranges_.back());
}

void PartitionPruner::RemovePartitionKeyRange(const string& bound) {
  if (bound.empty()) {
    partition_key_ranges_.clear();
    return;
  }

  for (auto range = partition_key_ranges_.rbegin();
       range != partition_key_ranges_.rend();
       range++) {

    if (bound > get<0>(*range)) {
      if (!get<1>(*range).empty() && bound >= get<1>(*range)) {
        partition_key_ranges_.pop_back();
      } else {
        get<0>(*range) = bound;
      }
    } else {
      break;
    }
  }
}

bool PartitionPruner::ShouldPrune(const Partition& partition) const {
  // range is an iterator that points to the first partition key range which
  // overlaps or is greater than the partition.
  auto range = lower_bound(partition_key_ranges_.rbegin(), partition_key_ranges_.rend(), partition,
    [] (const tuple<string, string>& scan_range, const Partition& partition) {
      // return true if scan_range < partition
      const string& scan_upper = get<1>(scan_range);
      return !scan_upper.empty() && scan_upper <= partition.partition_key_start();
    });

  bool prune = range == partition_key_ranges_.rend() ||
               (!partition.partition_key_end().empty() &&
                partition.partition_key_end() <= get<0>(*range));

  return prune;
}

std::string PartitionPruner::ToString(const Schema& schema,
                                      const PartitionSchema& partition_schema) const {
  vector<string> strings;
  for (auto range = partition_key_ranges_.rbegin();
       range != partition_key_ranges_.rend();
       range++) {
    strings.push_back(strings::Substitute(
          "[($0), ($1))",
          get<0>(*range).empty() ? "<start>" :
              partition_schema.PartitionKeyDebugString(get<0>(*range), schema),
          get<1>(*range).empty() ? "<end>" :
              partition_schema.PartitionKeyDebugString(get<1>(*range), schema)));
  }

  return JoinStrings(strings, ", ");
}

} // namespace kudu
