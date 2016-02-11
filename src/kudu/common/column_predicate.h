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

#include <boost/optional.hpp>
#include <string>

#include "kudu/common/row_key-util.h"
#include "kudu/common/schema.h"

namespace kudu {

class Arena;
class ColumnBlock;
class ColumnSchema;
class SelectionVector;
class TypeInfo;

enum class PredicateType {
  // A predicate which always evaluates to false.
  None,

  // A predicate which evaluates to true if the column value equals a known
  // value.
  Equality,

  // A predicate which evaluates to true if the column value falls within a
  // range.
  Range,
};

// A predicate which can be evaluated over a block of column values.
//
// A ColumnPredicate does not own the data to which it points internally,
// so it's lifetime must be managed to make sure it does not reference invalid
// data. Typically the lifetime of a ColumnPredicate will be tied to a scan (on
// the client side), or a scan iterator (on the server side).
class ColumnPredicate {
 public:

  // Creates a new equality predicate on the column and value.
  //
  // The value is not copied, and must outlive the returned predicate.
  static ColumnPredicate Equality(ColumnSchema column, const void* value);

  // Creates a new range column predicate from an inclusive lower bound and
  // exclusive upper bound.
  //
  // The values are not copied, and must outlive the returned predicate.
  static ColumnPredicate Range(ColumnSchema column, const void* lower, const void* upper);

  // Creates a new range column predicate from an inclusive lower bound and an
  // inclusive upper bound.
  //
  // The values are not copied, and must outlive the returned predicate. The
  // arena must outlive the returned predicate.
  //
  // If a normalized column predicate can not be created, then none will be
  // returned. This indicates that the predicate would cover the entire column
  // range.
  static boost::optional<ColumnPredicate> InclusiveRange(ColumnSchema column,
                                                         const void* lower,
                                                         const void* upper,
                                                         Arena* arena);

  // Creates a new predicate which matches no values.
  static ColumnPredicate None(ColumnSchema column);

  // Returns the type of this predicate.
  PredicateType predicate_type() const {
    return predicate_type_;
  }

  // Merge another predicate into this one.
  //
  // The other predicate must be on the same column.
  //
  // After a merge, this predicate will be the logical intersection of the
  // original predicates.
  //
  // Data is not copied from the other predicate, so it's data must continue
  // to outlive the merged predicate.
  //
  // If the predicates are disjoint, then an IllegalArgument status is returned.
  void Merge(const ColumnPredicate& other);

  // Evaluate the predicate on every row in the column block.
  //
  // This is evaluated as an 'AND' with the current contents of *sel:
  // - wherever the predicate evaluates false, set the appropriate bit in the selection
  //   vector to 0.
  // - If the predicate evalutes true, does not make any change to the
  //   selection vector.
  //
  // On any rows where the current value of *sel is false, the predicate evaluation
  // may be skipped.
  //
  // NOTE: the evaluation result is stored into '*sel' which may or may not be the
  // same vector as block->selection_vector().
  void Evaluate(const ColumnBlock& block, SelectionVector *sel) const;

  // Transition to a None predicate type.
  void SetToNone();

  // Print the predicate for debugging.
  std::string ToString() const;

  // Returns true if the column predicates are equivalent.
  bool operator==(const ColumnPredicate& other) const;

  // Returns the raw lower bound value if this is a range predicate, or the
  // equality value if this is an equality predicate.
  const void* raw_lower() const {
    return lower_;
  }

  // Returns the raw upper bound if this is a range predicate.
  const void* raw_upper() const {
    return upper_;
  }

  const ColumnSchema& column() const {
    return column_;
  }

 private:

  // Creates a new column predicate.
  ColumnPredicate(PredicateType predicate_type,
                  ColumnSchema column,
                  const void* lower,
                  const void* upper);

  // Merge another predicate into this range predicate.
  void MergeIntoRange(const ColumnPredicate& other);

  // Merge another predicate into this equality predicate.
  void MergeIntoEquality(const ColumnPredicate& other);

  // The type of this predicate.
  PredicateType predicate_type_;

  // The data type of the column. TypeInfo instances have a static lifetime.
  ColumnSchema column_;

  // The inclusive lower bound value if this is a Range predicate, or the
  // equality value if this is an Equality predicate.
  const void* lower_;

  // The exclusive upper bound value if this is a Range predicate.
  const void* upper_;
};

// Compares predicates according to selectivity.
//
// TODO: this could be improved with a histogram of expected values.
int SelectivityComparator(const ColumnPredicate& left, const ColumnPredicate& right);

} // namespace nudu
