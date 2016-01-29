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
#ifndef KUDU_COMMON_PREDICATE_ENCODER_H
#define KUDU_COMMON_PREDICATE_ENCODER_H

#include <gtest/gtest_prod.h>
#include <vector>

#include "kudu/common/encoded_key.h"
#include "kudu/common/row.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/util/auto_release_pool.h"
#include "kudu/util/status.h"

namespace kudu {

using std::vector;

// Encodes a list of column predicates into key-range predicates.
// Uses an AutoReleasePool to allocate EncodedKey instances,
// which means the lifetime of RangePredicateEncoder must be >= the
// lifetime of any classes that access the ScanSpec.
class RangePredicateEncoder {
 public:
  // 'key_schema' is not copied and must remain valid for the lifetime
  // of this object.
  //
  // Some parts of the resulting predicates may be allocated out of 'arena'
  // and thus 'arena' must not be reset or destructed until after any ScanSpecs
  // modified by this encoder have been destroyed.
  RangePredicateEncoder(const Schema* key_schema, Arena* arena);

  // Encodes the predicates found in 'spec' into a key range which is
  // then emitted back into 'spec'.
  //
  // If 'erase_pushed' is true, pushed predicates are removed from 'spec'.
  //
  // Returns Status::InvalidArgument if the scan spec would result in an empty
  // scan.
  Status EncodeRangePredicates(ScanSpec *spec, bool erase_pushed);

 private:
  friend class TestRangePredicateEncoder;
  FRIEND_TEST(CompositeIntKeysTest, TestSimplify);
  FRIEND_TEST(CompositeIntKeysTest, TestLiftPrimaryKeyBounds);

  struct SimplifiedBounds {
    SimplifiedBounds() : upper(nullptr), lower(nullptr) {}
    const void* upper;
    const void* lower;
    vector<int> orig_predicate_indexes;
  };

  Status SimplifyBounds(const ScanSpec& spec,
                        std::vector<SimplifiedBounds>* key_bounds) const;

  Status ConvertUpperBoundsToExclusive(const ScanSpec& spec,
                                       std::vector<SimplifiedBounds>* key_bounds) const;

  Status LiftPrimaryKeyBounds(const ScanSpec& spec,
                              std::vector<SimplifiedBounds>* key_bounds) const;

  // Checks that the Primary Key and Partition Key bounds of the scan spec are
  // coherent (lower < upper).
  Status CheckBoundsCoherency(const ScanSpec& spec) const;

  // Checks that the simplified predicate bounds are coherent (for each column,
  // lower < upper).
  Status CheckPredicateCoherency(const std::vector<SimplifiedBounds>& bounds) const;

  // Returns the number of contiguous equalities in the key prefix.
  int CountKeyPrefixEqualities(const std::vector<SimplifiedBounds>& bounds) const;
  int CountKeyPrefixEqualities(const EncodedKey& key_a,
                               const EncodedKey& key_b) const;

  // Erases any predicates we've encoded from the predicate list within the
  // ScanSpec.
  void ErasePushedPredicates(ScanSpec *spec, const std::vector<bool>& should_erase) const;

  const Schema* key_schema_;
  Arena* arena_;
  AutoReleasePool pool_;
};

} // namespace kudu

#endif
