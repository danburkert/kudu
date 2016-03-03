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

#include "kudu/common/key_util.h"

#include <boost/iterator/counting_iterator.hpp>
#include <iterator>
#include <string>
#include <tuple>
#include <type_traits>

#include "kudu/common/column_predicate.h"
#include "kudu/common/key_encoder.h"
#include "kudu/common/row.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/map-util.h"

using std::string;
using std::tuple;
using std::unordered_map;
using std::vector;

namespace kudu {
namespace key_util {

namespace {

template<DataType type>
bool IncrementIntCell(void* cell_ptr) {
  typedef DataTypeTraits<type> traits;
  typedef typename traits::cpp_type cpp_type;

  cpp_type orig;
  memcpy(&orig, cell_ptr, sizeof(cpp_type));

  cpp_type inc;
  if (std::is_unsigned<cpp_type>::value) {
    inc = orig + 1;
  } else {
    // Signed overflow is undefined in C. So, we'll use a branch here
    // instead of counting on undefined behavior.
    if (orig == MathLimits<cpp_type>::kMax) {
      inc = MathLimits<cpp_type>::kMin;
    } else {
      inc = orig + 1;
    }
  }
  memcpy(cell_ptr, &inc, sizeof(cpp_type));
  return inc > orig;
}

bool IncrementStringCell(void* cell_ptr, Arena* arena) {
  Slice orig;
  memcpy(&orig, cell_ptr, sizeof(orig));
  uint8_t* new_buf = CHECK_NOTNULL(
      static_cast<uint8_t*>(arena->AllocateBytes(orig.size() + 1)));
  memcpy(new_buf, orig.data(), orig.size());
  new_buf[orig.size()] = '\0';

  Slice inc(new_buf, orig.size() + 1);
  memcpy(cell_ptr, &inc, sizeof(inc));
  return true;
}

template<typename ColIdxIter>
void SetKeyToMinValues(ColIdxIter first, ColIdxIter last, ContiguousRow* row) {
  for (auto col_idx = first; col_idx != last; col_idx++) {
    DCHECK_LE(0, *col_idx);
    const ColumnSchema& col = row->schema()->column(*col_idx);
    col.type_info()->CopyMinValue(row->mutable_cell_ptr(*col_idx));
  }
}

// Increments a key with the provided column indices to the smalles key which is
// greater than the current key.
//
// For example, for a composite key with types (INT8, INT8), incrementing
// the row (1, 1) will result in (1, 2). Incrementing (1, 127) will result
// in (2, -128).
//
// Note that not all keys may be incremented without overflow. For example,
// if the key is an INT8, and the key is already set to '127', incrementing
// would overflow. In this case, the value is incremented and overflowed, but
// the function returns 'false' to indicate the overflow condition. Otherwise,
// returns 'true'.
//
// String and binary types are incremented by appending a '\0' byte to the end.
// Since our string and binary types have unbounded length, this implies that if
// a key has a string or binary component, it will always be incremented.
//
// For the case of incrementing string or binary types, we allocate a new copy
// from 'arena', which must be non-NULL.
//
// REQUIRES: all key columns must be initialized.
template<typename ColIdxIter>
bool IncrementKey(ColIdxIter first,
                  ColIdxIter last,
                  ContiguousRow* row,
                  Arena* arena) {
  for (auto col_idx = std::prev(last);
       std::distance(first, col_idx) >= 0;
       std::advance(col_idx, -1)) {
    if (IncrementCell(row->schema()->column(*col_idx), row->mutable_cell_ptr(*col_idx), arena)) {
      return true;
    }
  }
  return false;
}

template<typename ColIdxIter>
int PushUpperBoundKeyPredicates(ColIdxIter first,
                                ColIdxIter last,
                                const unordered_map<string, ColumnPredicate>& predicates,
                                ContiguousRow* row,
                                Arena* arena) {

  const Schema& schema = *CHECK_NOTNULL(row->schema());
  int pushed_predicates = 0;
  // Tracks whether the last pushed predicate is an equality predicate.
  const ColumnPredicate* final_predicate = nullptr;

  // Step 1: copy predicates into the row in key column order, stopping after
  // the first range predicate.

  for (auto col_idx = first; col_idx < last; std::advance(col_idx, 1)) {
    const ColumnSchema& column = schema.column(*col_idx);
    const ColumnPredicate* predicate = FindOrNull(predicates, column.name());
    if (predicate == nullptr) break;
    size_t size = column.type_info()->size();
    if (predicate->predicate_type() == PredicateType::Equality) {
      memcpy(row->mutable_cell_ptr(*col_idx), predicate->raw_lower(), size);
      pushed_predicates++;
      final_predicate = predicate;
    } else if (predicate->predicate_type() == PredicateType::Range) {
      if (predicate->raw_upper() != nullptr) {
        memcpy(row->mutable_cell_ptr(*col_idx), predicate->raw_upper(), size);
        pushed_predicates++;
        final_predicate = predicate;
      }
      // After the first column with a range constraint we stop pushing
      // constraints into the upper bound. Instead, we push minimum values
      // to the remaining columns (below), which is the maximally tight
      // constraint.
      break;
    } else {
      LOG(FATAL) << "unexpected predicate type can not be pushed into key";
    }
  }

  // If no predicates were pushed, no need to do any more work.
  if (pushed_predicates == 0) { return 0; }

  // Step 2: If the final predicate is an equality predicate, increment the
  // key to convert it to an exclusive upper bound.
  if (final_predicate->predicate_type() == PredicateType::Equality) {
    if (!IncrementKey(first, std::next(first, pushed_predicates), row, arena)) {
      // If the increment fails then this bound is is not constraining the keyspace.
      return 0;
    }
  }

  // Step 3: Fill the remaining columns without predicates with the min value.
  SetKeyToMinValues(std::next(first, pushed_predicates), last, row);
  return pushed_predicates;
}

template<typename ColIdxIter>
int PushLowerBoundKeyPredicates(ColIdxIter first,
                                ColIdxIter last,
                                const unordered_map<string, ColumnPredicate>& predicates,
                                ContiguousRow* row,
                                Arena* arena) {
  const Schema& schema = *CHECK_NOTNULL(row->schema());
  int pushed_predicates = 0;

  // Step 1: copy predicates into the row in key column order, stopping after
  // the first missing predicate.

  for (auto col_idx = first; col_idx < last; std::advance(col_idx, 1)) {
    const ColumnSchema& column = schema.column(*col_idx);
    const ColumnPredicate* predicate = FindOrNull(predicates, column.name());
    if (predicate == nullptr) break;
    size_t size = column.type_info()->size();
    if (predicate->predicate_type() == PredicateType::Equality) {
      memcpy(row->mutable_cell_ptr(*col_idx), predicate->raw_lower(), size);
      pushed_predicates++;
    } else if (predicate->predicate_type() == PredicateType::Range) {
      if (predicate->raw_lower() != nullptr) {
        memcpy(row->mutable_cell_ptr(*col_idx), predicate->raw_lower(), size);
        pushed_predicates++;
      } else {
        break;
      }
    } else {
      LOG(FATAL) << "unexpected predicate type can not be pushed into key";
    }
  }

  // If no predicates were pushed, no need to do any more work.
  if (pushed_predicates == 0) { return 0; }

  // Step 2: Fill the remaining columns without predicates with the min value.
  SetKeyToMinValues(std::next(first, pushed_predicates), last, row);
  return pushed_predicates;
}
} // anonymous namespace

bool IncrementPrimaryKey(ContiguousRow* row, Arena* arena) {
  int32_t num_pk_cols = row->schema()->num_key_columns();
  return IncrementKey(boost::make_counting_iterator(0),
                      boost::make_counting_iterator(num_pk_cols),
                      row,
                      arena);
}

bool IncrementCell(const ColumnSchema& col, void* cell_ptr, Arena* arena) {
  DataType type = col.type_info()->physical_type();
  switch (type) {
#define HANDLE_TYPE(t) case t: return IncrementIntCell<t>(cell_ptr);
    HANDLE_TYPE(UINT8);
    HANDLE_TYPE(UINT16);
    HANDLE_TYPE(UINT32);
    HANDLE_TYPE(UINT64);
    HANDLE_TYPE(INT8);
    HANDLE_TYPE(INT16);
    HANDLE_TYPE(INT32);
    HANDLE_TYPE(TIMESTAMP);
    HANDLE_TYPE(INT64);
    case UNKNOWN_DATA:
    case BOOL:
    case FLOAT:
    case DOUBLE:
      LOG(FATAL) << "Unable to handle type " << type << " in row keys";
    case STRING:
    case BINARY:
      return IncrementStringCell(cell_ptr, arena);
    default: CHECK(false) << "Unknown data type: " << type;
  }
  return false; // unreachable
#undef HANDLE_TYPE
}

int PushLowerBoundKeyPredicates(const vector<int32_t>& col_idxs,
                                const unordered_map<string, ColumnPredicate>& predicates,
                                ContiguousRow* row,
                                Arena* arena) {
  return PushLowerBoundKeyPredicates(col_idxs.begin(), col_idxs.end(), predicates, row, arena);
}

int PushUpperBoundKeyPredicates(const vector<int32_t>& col_idxs,
                                const unordered_map<string, ColumnPredicate>& predicates,
                                ContiguousRow* row,
                                Arena* arena) {
  return PushUpperBoundKeyPredicates(col_idxs.begin(), col_idxs.end(), predicates, row, arena);
}

int PushLowerBoundPrimaryKeyPredicates(const unordered_map<string, ColumnPredicate>& predicates,
                                       ContiguousRow* row,
                                       Arena* arena) {
  int32_t num_pk_cols = row->schema()->num_key_columns();
  return PushLowerBoundKeyPredicates(boost::make_counting_iterator(0),
                                     boost::make_counting_iterator(num_pk_cols),
                                     predicates,
                                     row,
                                     arena);
}

int PushUpperBoundPrimaryKeyPredicates(const unordered_map<string, ColumnPredicate>& predicates,
                                       ContiguousRow* row,
                                       Arena* arena) {
  int32_t num_pk_cols = row->schema()->num_key_columns();
  return PushUpperBoundKeyPredicates(boost::make_counting_iterator(0),
                                     boost::make_counting_iterator(num_pk_cols),
                                     predicates,
                                     row,
                                     arena);
}

void EncodeKey(const vector<int32_t>& col_idxs, const ContiguousRow& row, string* buffer) {
  for (int i = 0; i < col_idxs.size(); i++) {
    int32_t col_idx = col_idxs[i];
    const auto& encoder = GetKeyEncoder<string>(row.schema()->column(col_idx).type_info());
    encoder.Encode(row.cell_ptr(col_idx), i + 1 == col_idxs.size(), buffer);
  }
}

} // namespace key_util
} // namespace kudu
