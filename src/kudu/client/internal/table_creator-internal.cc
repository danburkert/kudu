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

#include "kudu/client/table_creator-internal.h"

#include <string>
#include <vector>

#include "kudu/client/client-internal.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/master/master.pb.h"

using std::string;
using std::vector;

namespace kudu {

using master::CreateTableRequestPB;
using master::CreateTableResponsePB;

namespace client {
namespace internal {

TableCreator::TableCreator(internal::Client* client)
  : client_(client),
    schema_(nullptr),
    num_replicas_(0),
    wait_(true) {
}

TableCreator::~TableCreator() {
  STLDeleteElements(&range_partition_splits_);
}

HashPartitionCreator TableCreator::add_hash_partition() {
  return HashPartitionCreator(partition_schema_.add_hash_bucket_schemas());
}

void TableCreator::add_range_partition_column(string column) {
  partition_schema_.mutable_range_schema()->add_columns()->set_name(std::move(column));
}

void TableCreator::clear_range_partition_columns() {
  partition_schema_.mutable_range_schema()->clear_columns();
}

void TableCreator::clear_range_partition_splits() {
  std::vector<const KuduPartialRow*> splits;
  STLDeleteElements(&splits);
  splits.swap(range_partition_splits_);
}

Status TableCreator::Create() {
  if (!table_name_.length()) {
    return Status::InvalidArgument("Missing table name");
  }
  if (!schema_) {
    return Status::InvalidArgument("Missing schema");
  }

  // Build request.
  CreateTableRequestPB req;
  req.set_name(table_name_);
  if (num_replicas_ >= 1) {
    req.set_num_replicas(num_replicas_);
  }
  RETURN_NOT_OK_PREPEND(SchemaToPB(*schema_, req.mutable_schema()), "Invalid schema");

  RowOperationsPBEncoder encoder(req.mutable_split_rows());

  for (const KuduPartialRow* row : range_partition_splits_) {
    encoder.Add(RowOperationsPB::SPLIT_ROW, *row);
  }
  req.mutable_partition_schema()->CopyFrom(partition_schema_);

  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  if (timeout_.Initialized()) {
    deadline.AddDelta(timeout_);
  } else {
    deadline.AddDelta(client_->default_admin_operation_timeout());
  }

  RETURN_NOT_OK_PREPEND(client_->CreateTable(req, *schema_, deadline),
                        strings::Substitute("Error creating table $0 on the master", table_name_));

  // Spin until the table is fully created, if requested.
  if (wait_) {
    RETURN_NOT_OK(client_->WaitForCreateTableToFinish(table_name_, deadline));
  }

  return Status::OK();
}

} // namespace internal
} // namespace client
} // namespace kudu
