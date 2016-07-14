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

#include <mutex>

#include "kudu/client/batcher.h"
#include "kudu/client/callbacks.h"
#include "kudu/client/error_collector.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/messenger.h"
#include "kudu/util/flag_tags.h"

namespace kudu {

using rpc::Messenger;

namespace client {

using internal::Batcher;
using internal::ErrorCollector;

using sp::shared_ptr;
using sp::weak_ptr;


KuduSession::Data::Data(shared_ptr<KuduClient> client,
                        std::shared_ptr<rpc::Messenger> messenger)
    : client_(std::move(client)),
      messenger_(std::move(messenger)),
      error_collector_(new ErrorCollector()),
      external_consistency_mode_(CLIENT_PROPAGATED),
      timeout_ms_(-1),
      flush_interval_(MonoDelta::FromMilliseconds(1000)),
      flush_task_active_(false),
      flush_mode_(AUTO_FLUSH_SYNC),
      flow_control_cond_(&flow_control_cond_mutex_),
      buffer_bytes_limit_(7 * 1024 * 1024),
      buffer_watermark_pct_(80),
      total_bytes_used_(0),
      flush_pending_bytes_(0),
      buffer_extra_flush_enabled_(true) {
}

void KuduSession::Data::Init(const weak_ptr<KuduSession>& session) {
  NextBatcher(session, 0, nullptr);
  CheckRunMaxWaitFlushTask(session);
}

void KuduSession::Data::FlushFinished(Batcher* batcher) {
  const int64_t bytes_flushed = batcher->buffer_bytes_used();
  {
    std::lock_guard<simple_spinlock> l(lock_);
    CHECK_EQ(flushed_batchers_.erase(batcher), 1);
  }
  {
    std::lock_guard<Mutex> l(flow_control_cond_mutex_);
    total_bytes_used_ -= bytes_flushed;
    flush_pending_bytes_ -= bytes_flushed;
    // The the logic of KuduSession::ApplyWriteOp() needs to know
    // if the byte count changes. There must be no more than one thread waiting
    // on the flow_control_cond_ condition variable.
    flow_control_cond_.Signal();
  }
}

Status KuduSession::Data::Close(bool force) {
  std::lock_guard<simple_spinlock> l(lock_);
  if (!batcher_) {
      return Status::OK();
  }
  if (batcher_->HasPendingOperations() && !force) {
    return Status::IllegalState("Could not close. There are pending operations.");
  }
  batcher_->Abort();

  return Status::OK();
}

Status KuduSession::Data::SetFlushMode(
    FlushMode mode,
    const sp::weak_ptr<KuduSession>& session) {
  if (HasPendingOperations()) {
    return Status::IllegalState(
          "Cannot change flush mode when writes are buffered.");
  }
  {
    std::lock_guard<simple_spinlock> l(lock_);
    flush_mode_ = mode;
  }
  CheckRunMaxWaitFlushTask(session);

  return Status::OK();
}

Status KuduSession::Data::SetBufferBytesLimit(size_t size) {
  if (HasPendingOperations()) {
    return Status::IllegalState(
          "Cannot change buffer size limit when writes are buffered.");
  }
  std::lock_guard<Mutex> l(flow_control_cond_mutex_);
  buffer_bytes_limit_ = size;
  return Status::OK();
}

Status KuduSession::Data::SetBufferFlushWatermark(int watermark_pct) {
  if (watermark_pct < 0 || watermark_pct > 100) {
    return Status::InvalidArgument(
        strings::Substitute("$0: watermark must be between 0 and 100 inclusive",
                            watermark_pct));
  }
  if (HasPendingOperations()) {
    return Status::IllegalState(
          "Cannot change buffer flush watermark when writes are buffered.");
  }
  std::lock_guard<Mutex> l(flow_control_cond_mutex_);
  buffer_watermark_pct_ = watermark_pct;
  return Status::OK();
}

Status KuduSession::Data::SetBufferFlushInterval(unsigned int millis) {
  if (HasPendingOperations()) {
    return Status::IllegalState(
          "Cannot change buffer flush interval when writes are buffered.");
  }
  std::lock_guard<simple_spinlock> l(lock_);
  flush_interval_ = MonoDelta::FromMilliseconds(millis);
  return Status::OK();
}

void KuduSession::Data::SetTimeoutMillis(int timeout_ms) {
  if (timeout_ms < 0) {
    timeout_ms = 0;
  }
  std::lock_guard<Mutex> l(flow_control_cond_mutex_);
  timeout_ms_ = timeout_ms;
  if (batcher_) {
    batcher_->SetTimeoutMillis(timeout_ms);
  }
}

void KuduSession::Data::FlushAsync(const weak_ptr<KuduSession>& session,
                                   KuduStatusCallback* cb) {
  NextBatcher(session, 0, cb);
}

bool KuduSession::Data::HasPendingOperations() const {
  std::lock_guard<simple_spinlock> l(lock_);
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

int KuduSession::Data::CountBufferedOperations() const {
  std::lock_guard<simple_spinlock> l(lock_);
  if (batcher_) {
    // Prior batchers (if any) with pending operations are not relevent here:
    // the flushed operations, even if they have not reached the tablet server,
    // are not considered "buffered".
    return batcher_->CountBufferedOperations();
  } else {
    return 0;
  }
}

void KuduSession::Data::NextBatcher(const weak_ptr<KuduSession>& session,
                                    int64_t watermark,
                                    KuduStatusCallback* cb) {
  scoped_refptr<Batcher> old_batcher;
  {
    std::lock_guard<simple_spinlock> l(lock_);
    if (batcher_ && batcher_->buffer_bytes_used() < watermark) {
      // Do not move to the next batcher if the watermark has not reached yet.
      return;
    }
    scoped_refptr<Batcher> batcher(
      new Batcher(client_.get(), error_collector_.get(), session,
                  external_consistency_mode_));
    if (timeout_ms_ != -1) {
      batcher->SetTimeoutMillis(timeout_ms_);
    }
    batcher.swap(batcher_);
    old_batcher.swap(batcher);
    if (old_batcher) {
      InsertOrDie(&flushed_batchers_, old_batcher.get());
    }
  }

  if (old_batcher) {
    {
      // Update information on the number of bytes being flushed.
      // Once operations are sent to the server and server reports
      // on their status, this counter is decreased on the same amount.
      std::lock_guard<Mutex> l(flow_control_cond_mutex_);
      flush_pending_bytes_ += old_batcher->buffer_bytes_used();
    }
    // Send off any buffered data. Important to do this outside of the lock
    // since the callback may itself try to take the lock, in the case that
    // the batch fails "inline" on the same thread.
    old_batcher->FlushAsync(cb);
  }
}

Status KuduSession::Data::ApplyWriteOp(
    const sp::weak_ptr<KuduSession>& weak_session,
    KuduWriteOperation* write_op,
    FlushMode* flush_mode) {

  if (write_op == nullptr) {
    return Status::InvalidArgument("nil operation argument");
  }
  if (!write_op->row().IsKeySet()) {
    Status status = Status::IllegalState(
        "Key not specified", write_op->ToString());
    error_collector_->AddError(
        gscoped_ptr<KuduError>(new KuduError(write_op, status)));
    return status;
  }

  // Make sure calls to this method are serialized if coming through the same
  // KuduSession object.
  std::lock_guard<simple_spinlock> call_serializer(apply_write_op_lock_);

  FlushMode current_flush_mode;
  {
    std::lock_guard<simple_spinlock> l(lock_);
    current_flush_mode = flush_mode_;
  }

  int64_t flush_watermark;
  bool need_pre_flush = false;
  do {
    RETURN_NOT_OK(CanApplyWriteOp(write_op, current_flush_mode,
                                  &need_pre_flush, &flush_watermark));
    if (need_pre_flush) {
      NextBatcher(weak_session, flush_watermark, nullptr);
    }
  } while (need_pre_flush);
  {
    Status s;
    {
      std::lock_guard<simple_spinlock> l(lock_);
      s = batcher_->Add(write_op);
      if (PREDICT_FALSE(!s.ok())) {
        error_collector_->AddError(
            gscoped_ptr<KuduError>(new KuduError(write_op, s)));
      }
    }
    if (PREDICT_FALSE(!s.ok())) {
      // Since the operation hasn't been added, it's necessary to rollback
      // its buffer space reservation.
      std::lock_guard<Mutex> l(flow_control_cond_mutex_);
      total_bytes_used_ -= Batcher::GetOperationSizeInBuffer(write_op);
      // Since there must be no more than one thread calling the
      // KuduSession::Apply() method and the ApplyWriteOp() is called only
      // from that KuduSession::Apply(), no thread should be waiting on
      // the flow_control_cond_ in CanApplyWriteOp() at this point,
      // so no need to notify on the variable change.
      return s;
    }
  }

  if (current_flush_mode == AUTO_FLUSH_BACKGROUND) {
    // Check whether the percentage of the freshly added
    // (i.e. not yet scheduled for pushing to the server) operations
    // is over the high-watermark level. If yes, it's necessary to flush
    // the accumulated write operations. The freshly added operations
    // are put into the current batcher. All other batchers, if any,
    // contain operations which have been scheduled for flush or
    // operations which are being flushed.
    NextBatcher(weak_session, flush_watermark, nullptr);
  }
  if (flush_mode != nullptr) {
    *flush_mode = current_flush_mode;
  }

  return Status::OK();
}

int64_t KuduSession::Data::GetPendingOperationsSize() const {
  std::lock_guard<simple_spinlock> l(lock_);
  int64_t total = 0;
  if (batcher_) {
    total += batcher_->buffer_bytes_used();
  }
  for (const auto b : flushed_batchers_) {
    total += b->buffer_bytes_used();
  }
  return total;
}

void KuduSession::Data::CheckRunMaxWaitFlushTask(
    sp::weak_ptr<KuduSession> weak_session) {
  KuduSession::Data::MaxWaitFlushTask(
      Status::OK(), messenger_, weak_session, false);
}

void KuduSession::Data::MaxWaitFlushTask(
    const Status& status,
    std::weak_ptr<rpc::Messenger> weak_messenger,
    sp::weak_ptr<KuduSession> weak_session,
    bool self_rescheduled) {
  if (PREDICT_FALSE(!status.ok())) {
    return;
  }
  // Check that the session is still alive to access the data safely.
  sp::shared_ptr<KuduSession> session(weak_session.lock());
  if (PREDICT_FALSE(!session)) {
    return;
  }

  KuduSession::Data* data = session->data_;
  bool needs_flush = false;
  MonoDelta schedule_interval;
  {
    std::lock_guard<simple_spinlock> l(data->lock_);
    if (!self_rescheduled && data->flush_task_active_) {
      // The task is already active.
      return;
    }
    if (data->flush_mode_ == AUTO_FLUSH_BACKGROUND) {
      data->flush_task_active_ = true;
    } else {
      // Flush mode could change during the operation. If current mode
      // is no longer AUTO_FLUSH_BACKGROUND, do not re-schedule the task.
      data->flush_task_active_ = false;
      return;
    }
    // The idea is to flush the batcher when its age is very close to the
    // flush_interval: let's call it 'batcher flush age'. If current batcher
    // hasn't reached its flush age yet, just re-schedule the task to
    // re-evaluate the age of then-will-be-active batcher, so if the current
    // batcher is current at that time, it will be exactly of its flush age.
    schedule_interval = data->flush_interval_;
    const MonoTime first_op_time = data->batcher_->first_op_time();
    if (PREDICT_TRUE(first_op_time.Initialized())) {
      const MonoTime now = MonoTime::Now();
      if (first_op_time + schedule_interval <= now) {
        needs_flush = true;
      } else {
        schedule_interval = first_op_time + schedule_interval - now;
      }
    }
  }

  if (needs_flush) {
    // Detach the batcher, schedule its flushing,
    // and install a new one to accumulate incoming write operations.
    data->NextBatcher(session, 1, nullptr);
  }

  // Re-schedule the task to check and flush the batcher when its age is closer
  // to the flush_interval.
  std::shared_ptr<rpc::Messenger> messenger(weak_messenger.lock());
  if (PREDICT_TRUE(messenger)) {
    messenger->ScheduleOnReactor(
        boost::bind(&KuduSession::Data::MaxWaitFlushTask,
                    _1, messenger, session, true),
        schedule_interval);
  }
}

Status KuduSession::Data::CheckBufferLimit(size_t required_size) const {
  flow_control_cond_mutex_.AssertAcquired();
  if (required_size > buffer_bytes_limit_) {
    return Status::Incomplete(strings::Substitute(
          "buffer size limit is too small to fit operation: "
          "required $0, size limit $1",
          HumanReadableNumBytes::ToString(required_size),
          HumanReadableNumBytes::ToString(buffer_bytes_limit_)));
  }
  return Status::OK();
}

Status KuduSession::Data::CanApplyWriteOp(KuduWriteOperation* write_op,
                                          KuduSession::FlushMode flush_mode,
                                          bool* need_pre_flush,
                                          int64_t* flush_watermark) {
  const int64_t required_size = Batcher::GetOperationSizeInBuffer(write_op);

  std::lock_guard<Mutex> l(flow_control_cond_mutex_);
  // A sanity check: before trying to validate against any run-time metrics,
  // verify that the single operation can fit into an empty buffer
  // given the restriction on the buffer size.
  Status s = CheckBufferLimit(required_size);
  if (PREDICT_FALSE(!s.ok())) {
    error_collector_->AddError(
        gscoped_ptr<KuduError>(new KuduError(write_op, s)));
    return s;
  }
  *need_pre_flush = false;
  *flush_watermark = buffer_bytes_limit_ * buffer_watermark_pct_ / 100;

  bool reserved = false;
  if (flush_mode == AUTO_FLUSH_BACKGROUND) {
    if (PREDICT_TRUE(buffer_extra_flush_enabled_) &&
        (total_bytes_used_ - flush_pending_bytes_ + required_size >
         buffer_bytes_limit_)) {
      // In need of an extra flush in this case because otherwise
      // it will require waiting for the max-wait flush to happen, which might
      // delay the flow of incoming write operations. This case is illustrated
      // by the diagram below. The diagram shows the data layout in the
      // buffer, the flush watermark, and the incoming write operation:
      //
      //                                             +-----requied_size-----+
      //                                             | Data of the write    |
      //                   +--buffer_bytes_limit_-+  | operation to add.    |
      // flush watermark ->|                      |  |                      |
      //                   +---total_bytes_used_--+  +----------0-----------+
      //                   |                      |
      //                   | Data of fresh (newly |
      //                   | added) operations.   |
      //                   |                      |
      //                   +-flush_pending_bytes_-+
      //                   |                      |
      //                   | Data of operations   |
      //                   | being flushed now.   |
      //                   |                      |
      //                   +----------0-----------+
      //
      *flush_watermark = total_bytes_used_ - flush_pending_bytes_;
      *need_pre_flush = true;
      return Status::OK();
    }

    // In AUTO_FLUSH_BACKGROUND mode Apply() blocks if total would-be-used
    // buffer space is over the limit. Once amount of buffered data drops
    // below the limit, a blocking call to Aplly() is unblocked.
    bool over_limit = (total_bytes_used_ + required_size) > buffer_bytes_limit_;
    while (over_limit) {
      flow_control_cond_.Wait();
      // Check whether the total size of the buffered operations
      // has dropped to accommodate a new operation. If true, then stop
      // waiting/blocking and proceeed with the pending operation.
      over_limit = (total_bytes_used_ + required_size) > buffer_bytes_limit_;
      if (!over_limit) {
        total_bytes_used_ += required_size;
        reserved = true;
        break;
      }
    }
  }
  if (!reserved) {
    total_bytes_used_ += required_size;
    reserved = true;
  }

  if (PREDICT_FALSE(total_bytes_used_ > buffer_bytes_limit_)) {
    DCHECK(flush_mode != AUTO_FLUSH_BACKGROUND);
    total_bytes_used_ -= required_size;
    Status s = Status::Incomplete(strings::Substitute(
        "not enough space remaining in buffer for operation: "
        "required $0, $1 already used (buffer size limit $2)",
        HumanReadableNumBytes::ToString(required_size),
        HumanReadableNumBytes::ToString(total_bytes_used_),
        HumanReadableNumBytes::ToString(buffer_bytes_limit_)));
    error_collector_->AddError(
        gscoped_ptr<KuduError>(new KuduError(write_op, s)));
    return s;
  }

  return Status::OK();
}

} // namespace client
} // namespace kudu
