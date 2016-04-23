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
#ifndef KUDU_CLIENT_TABLE_INTERNAL_H
#define KUDU_CLIENT_TABLE_INTERNAL_H

#include <string>
#include <memory>

#include "kudu/client/schema.h"
#include "kudu/common/partition.h"

namespace kudu {
namespace client {
namespace internal {

class Table {
 public:
  Table(std::shared_ptr<Client> client,
        std::string name,
        std::string table_id,
        const KuduSchema& schema,
        PartitionSchema partition_schema);
  ~Table();

  Status Open();

  std::shared_ptr<Client> client_;

  std::string name_;
  const std::string id_;

  // TODO: figure out how we deal with a schema change from the client perspective.
  // Do we make them call a RefreshSchema() method? Or maybe reopen the table and get
  // a new KuduTable instance (which would simplify the object lifecycle a little?)
  const KuduSchema schema_;
  const PartitionSchema partition_schema_;

  DISALLOW_COPY_AND_ASSIGN(Table);
};

} // namespace internal
} // namespace client
} // namespace kudu

#endif
