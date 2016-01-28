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

#include "kudu/common/scan_optimizer.h"

#include <vector>

#include "kudu/common/partial_row.h"
#include "kudu/common/scan_predicate.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/util/status.h"

using std::vector;

namespace kudu {

Status ScanOptimizer::OptimizeScan(const Schema& schema, ScanSpec* spec) {
  KuduPartialRow lower_bounds(&schema);
  KuduPartialRow upper_bounds(&schema);

  SimplifyPredicates(spec->mutable_predicates(), &lower_bounds, &upper_bounds);

  return Status::OK();
}

Status ScanOptimizer::SimplifyPredicates(vector<ColumnRangePredicate>* predicates,
                                         KuduPartialRow* lower_bounds,
                                         KuduPartialRow* upper_bounds) {
  CHECK_NOTNULL(lower_bounds);
  CHECK_NOTNULL(upper_bounds);

  for (auto predicate = predicates->begin(); predicate != predicates->end(); predicate++) {
    const ColumnSchema& column = predicate->column();
    int32_t col_idx = lower_bounds->schema()->find_column(column.name());

    CHECK(col_idx != Schema::kColumnNotFound);
    CHECK(predicate->range().has_lower_bound() || predicate->range().has_upper_bound());

    if (predicate->range().has_lower_bound() &&
        (!lower_bounds->IsColumnSet(col_idx) ||
         column.type_info()->Compare(predicate->range().lower_bound(),
                                     lower_bounds->Get(col_idx)) > 0)) {
      RETURN_NOT_OK(lower_bounds->Set(col_idx,
                                      static_cast<const uint8_t*>(predicate->range().lower_bound()),
                                      false));
    }

    if (predicate->range().has_upper_bound() &&
        (!upper_bounds->IsColumnSet(col_idx) ||
         column.type_info()->Compare(predicate->range().upper_bound(),
                                     upper_bounds->Get(col_idx)) < 0)) {
      RETURN_NOT_OK(upper_bounds->Set(col_idx,
                                      static_cast<const uint8_t*>(predicate->range().upper_bound()),
                                      false));
    }
  }

  return Status::OK();
}

Status ScanOptimizer::LiftPrimaryKeyBounds(const EncodedKey* begin_key,
                                           const EncodedKey* exclusive_end_key,
                                           KuduPartialRow* lower_bounds,
                                           KuduPartialRow* upper_bounds) {

}


} // namespace kudu
