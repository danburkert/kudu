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

#include "kudu/client/table_alterer-internal.h"

#include <string>

#include "kudu/client/client-internal.h"
#include "kudu/client/schema-internal.h"
#include "kudu/client/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/master/master.pb.h"

using std::string;

namespace kudu {
namespace client {

using master::AlterTableRequestPB;
using master::AlterTableRequestPB_AlterColumn;

KuduTableAlterer::Data::Data(KuduClient* client, string name)
    : client_(client),
      table_name_(std::move(name)),
      wait_(true) {
}

KuduTableAlterer::Data::~Data() {
  for (Step& s : steps_) {
    delete s.spec;
  }
}

Status KuduTableAlterer::Data::ToRequest(AlterTableRequestPB* req) {
  if (!status_.ok()) {
    return status_;
  }

  if (!rename_to_.is_initialized() &&
      steps_.empty()) {
    return Status::InvalidArgument("No alter steps provided");
  }

  req->Clear();
  req->mutable_table()->set_table_name(table_name_);
  if (rename_to_.is_initialized()) {
    req->set_new_table_name(rename_to_.get());
  }

  for (const Step& s : steps_) {
    AlterTableRequestPB::Step* pb_step = req->add_alter_schema_steps();
    pb_step->set_type(s.step_type);

    switch (s.step_type) {
      case AlterTableRequestPB::ADD_COLUMN:
      {
        KuduColumnSchema col;
        RETURN_NOT_OK(s.spec->ToColumnSchema(&col));
        ColumnSchemaToPB(*col.col_,
                         pb_step->mutable_add_column()->mutable_schema());
        break;
      }
      case AlterTableRequestPB::DROP_COLUMN:
      {
        pb_step->mutable_drop_column()->set_name(s.spec->data_->name);
        break;
      }
      case AlterTableRequestPB::ALTER_COLUMN:
        // TODO(KUDU-861): support altering a column in the wire protocol.
        // For now, we just give an error if the caller tries to do
        // any operation other than rename.
        if (s.spec->data_->has_type ||
            s.spec->data_->has_encoding ||
            s.spec->data_->has_compression ||
            s.spec->data_->has_nullable ||
            s.spec->data_->primary_key ||
            s.spec->data_->has_default ||
            s.spec->data_->default_val ||
            s.spec->data_->remove_default) {
          return Status::NotSupported("cannot support AlterColumn of this type",
                                      s.spec->data_->name);
        }
        // We only support rename column
        if (!s.spec->data_->has_rename_to) {
          return Status::InvalidArgument("no alter operation specified",
                                         s.spec->data_->name);
        }
        pb_step->mutable_rename_column()->set_old_name(s.spec->data_->name);
        pb_step->mutable_rename_column()->set_new_name(s.spec->data_->rename_to);
        pb_step->set_type(AlterTableRequestPB::RENAME_COLUMN);
        break;
      default:
        LOG(FATAL) << "unknown step type " << s.step_type;
    }
  }

  return Status::OK();
}

void KuduTableAlterer::Data::RenameTo(const string& new_name) {
  rename_to_ = new_name;
}

KuduColumnSpec* KuduTableAlterer::Data::AddColumn(const string& name) {
  Data::Step s = {AlterTableRequestPB::ADD_COLUMN,
                  new KuduColumnSpec(name)};
  steps_.push_back(s);
  return s.spec;
}

KuduColumnSpec* KuduTableAlterer::Data::AlterColumn(const string& name) {
  Data::Step s = {AlterTableRequestPB::ALTER_COLUMN,
                  new KuduColumnSpec(name)};
  steps_.push_back(s);
  return s.spec;
}

void KuduTableAlterer::Data::DropColumn(const string& name) {
  Step s = {AlterTableRequestPB::DROP_COLUMN, new KuduColumnSpec(name)};
  steps_.push_back(s);
}

Status KuduTableAlterer::Data::Alter() {
  AlterTableRequestPB req;
  RETURN_NOT_OK(ToRequest(&req));

  MonoDelta timeout = timeout_.Initialized() ? timeout_ :
      client_->default_admin_operation_timeout();
  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(timeout);
  RETURN_NOT_OK(client_->data_->AlterTable(client_, req, deadline));
  if (wait_) {
    string alter_name = rename_to_.get_value_or(table_name_);
    RETURN_NOT_OK(client_->data_->WaitForAlterTableToFinish(client_, alter_name, deadline));
  }

  return Status::OK();
}

void KuduTableAlterer::Data::timeout(const MonoDelta& timeout) {
  timeout_ = timeout;
}

void KuduTableAlterer::Data::wait(bool wait) {
  wait_ = wait;
}

} // namespace client
} // namespace kudu
