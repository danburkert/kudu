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

#include "kudu/client/session-internal.h"

#include <memory>

#include "kudu/client/batcher.h"
#include "kudu/client/callbacks.h"
#include "kudu/client/error_collector.h"
#include "kudu/gutil/map-util.h"

using std::shared_ptr;

namespace kudu {
namespace client {
namespace internal {

Session::Session(std::shared_ptr<Client> client)
    : client_(std::move(client)),
      error_collector_(new ErrorCollector()),
      flush_mode_(KuduSession::AUTO_FLUSH_SYNC),
      external_consistency_mode_(KuduSession::CLIENT_PROPAGATED),
      timeout_ms_(-1) {
}

void Session::Init() {
  lock_guard<simple_spinlock> l(&lock_);
  CHECK(!batcher_);
  NewBatcher(nullptr);
}

Session::~Session() {
}

Status Session::SetFlushMode(KuduSession::FlushMode m) {
  if (m == KuduSession::AUTO_FLUSH_BACKGROUND) {
    return Status::NotSupported("AUTO_FLUSH_BACKGROUND has not been implemented in the"
        " c++ client (see KUDU-456).");
  }
  if (batcher_->HasPendingOperations()) {
    // TODO: there may be a more reasonable behavior here.
    return Status::IllegalState("Cannot change flush mode when writes are buffered");
  }

  flush_mode_ = m;
  return Status::OK();
}

Status Session::SetExternalConsistencyMode(KuduSession::ExternalConsistencyMode m) {
  if (batcher_->HasPendingOperations()) {
    // TODO: there may be a more reasonable behavior here.
    return Status::IllegalState("Cannot change external consistency mode when writes are buffered");
  }

  external_consistency_mode_ = m;
  return Status::OK();
}

void Session::SetTimeoutMillis(int millis) {
  CHECK_GE(millis, 0);
  timeout_ms_ = millis;
  batcher_->SetTimeoutMillis(millis);
}

Status Session::Apply(KuduWriteOperation* write_op) {
  if (!write_op->row().IsKeySet()) {
    Status status = Status::IllegalState("Key not specified", write_op->ToString());
    error_collector_->AddError(gscoped_ptr<KuduError>(new KuduError(write_op, status)));
    return status;
  }

  Status s = batcher_->Add(write_op);
  if (!PREDICT_FALSE(s.ok())) {
    error_collector_->AddError(gscoped_ptr<KuduError>(new KuduError(write_op, s)));
    return s;
  }

  if (flush_mode_ == KuduSession::AUTO_FLUSH_SYNC) {
    return Flush();
  }

  return Status::OK();
}

Status Session::Flush() {
  Synchronizer s;
  KuduStatusMemberCallback<Synchronizer> ksmcb(&s, &Synchronizer::StatusCB);
  FlushAsync(&ksmcb);
  return s.Wait();
}

void Session::FlushAsync(KuduStatusCallback* user_callback) {
  CHECK_NE(flush_mode_, KuduSession::AUTO_FLUSH_BACKGROUND)
      << "AUTO_FLUSH_BACKGROUND has not been implemented";

  // Swap in a new batcher to start building the next batch.
  // Save off the old batcher.
  scoped_refptr<Batcher> old_batcher;
  {
    lock_guard<simple_spinlock> l(&lock_);
    NewBatcher(&old_batcher);
    InsertOrDie(&flushed_batchers_, old_batcher.get());
  }

  // Send off any buffered data. Important to do this outside of the lock
  // since the callback may itself try to take the lock, in the case that
  // the batch fails "inline" on the same thread.
  old_batcher->FlushAsync(user_callback);
}

void Session::NewBatcher(scoped_refptr<Batcher>* old_batcher) {
  DCHECK(lock_.is_locked());

  shared_ptr<Session> shared = shared_from_this();

  scoped_refptr<Batcher> batcher(
    new Batcher(client_.get(), error_collector_.get(), shared_from_this(),
                external_consistency_mode_));
  if (timeout_ms_ != -1) {
    batcher->SetTimeoutMillis(timeout_ms_);
  }
  batcher.swap(batcher_);

  if (old_batcher) {
    old_batcher->swap(batcher);
  }
}

void Session::FlushFinished(Batcher* batcher) {
  lock_guard<simple_spinlock> l(&lock_);
  CHECK_EQ(flushed_batchers_.erase(batcher), 1);
}

Status Session::Close(bool force) {
  if (batcher_->HasPendingOperations() && !force) {
    return Status::IllegalState("Could not close. There are pending operations.");
  }
  batcher_->Abort();
  return Status::OK();
}

bool Session::HasPendingOperations() const {
  lock_guard<simple_spinlock> l(&lock_);
  if (batcher_->HasPendingOperations()) {
    return true;
  }
  for (Batcher* b : flushed_batchers_) {
    if (b->HasPendingOperations()) {
      return true;
    }
  }
  return false;
}

int Session::CountBufferedOperations() const {
  lock_guard<simple_spinlock> l(&lock_);
  CHECK_EQ(flush_mode_, KuduSession::MANUAL_FLUSH);

  return batcher_->CountBufferedOperations();
}

} // namespace internal
} // namespace client
} // namespace kudu
