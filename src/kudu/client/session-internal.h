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
#ifndef KUDU_CLIENT_SESSION_INTERNAL_H
#define KUDU_CLIENT_SESSION_INTERNAL_H

#include <memory>
#include <unordered_set>

#include "kudu/client/client.h"
#include "kudu/gutil/map-util.h"
#include "kudu/util/atomic.h"
#include "kudu/util/locks.h"

namespace kudu {

namespace rpc {
class Messenger;
} // namespace rpc

namespace client {

namespace internal {
class Batcher;
class ErrorCollector;
} // internal

class KuduSession::Data {
 public:
  explicit Data(sp::shared_ptr<KuduClient> client,
                std::shared_ptr<rpc::Messenger> messenger);

  void Init(const sp::weak_ptr<KuduSession>& session);

  // Called by Batcher when a flush has finished.
  void FlushFinished(internal::Batcher* b);

  // Returns Status::IllegalState() if 'force' is false and there are still pending
  // operations. If 'force' is true batcher_ is aborted even if there are pending
  // operations.
  Status Close(bool force);

  // Set flush mode for the session.
  Status SetFlushMode(FlushMode mode, const sp::weak_ptr<KuduSession>& session);

  // Set limit on buffer space consumed by buffered write operations.
  Status SetBufferBytesLimit(size_t size);

  // Set buffer flush watermark (in percentage of the total buffer space).
  Status SetBufferFlushWatermark(int32_t watermark_pct);

  // Set the interval of the background max-wait flushing (in milliseconds).
  Status SetBufferFlushInterval(unsigned int period_ms);

  // Set timeout for write operations, in milliseconds.
  void SetTimeoutMillis(int timeout_ms);

  void FlushAsync(const sp::weak_ptr<KuduSession>& session,
                  KuduStatusCallback* cb);

  // Check whether there are operations not yet sent to tablet servers.
  bool HasPendingOperations() const;

  // Get total number of buffered operations.
  int CountBufferedOperations() const;

  // Swap current buffer with new one and asynchronously flush the former
  // if size of the buffered operations is at or over the specified watermark.
  // The specified callback will be called with the flush result.
  void NextBatcher(const sp::weak_ptr<KuduSession>& session,
                   int64_t watermark, KuduStatusCallback* cb);

  // Check whether it's possible to apply write operation, i.e. push it through
  // the batcher chain. On successful return, the output flush_mode parameter
  // is set to the effective flush mode.
  Status ApplyWriteOp(const sp::weak_ptr<KuduSession>& session,
                      KuduWriteOperation* write_op, FlushMode* flush_mode);

  // Get the total size of pending (i.e. both freshly added and
  // in process of being flushed) operations. This method is used by tests.
  int64_t GetPendingOperationsSize() const;

  void CheckRunMaxWaitFlushTask(sp::weak_ptr<KuduSession> weak_session);

  // The self-rescheduling task to flush write operations which have been
  // accumulating for too long compared with flush_interval_.
  // This does real work only in case of AUTO_FLUSH_BACKGROUND mode.
  static void MaxWaitFlushTask(const Status& status,
                               std::weak_ptr<rpc::Messenger> weak_messenger,
                               sp::weak_ptr<KuduSession> weak_session,
                               bool self_rescheduled);

  // Check if the specified size fits buffer given its current size limit.
  Status CheckBufferLimit(size_t required_size) const;

  // Check whether it's possible to apply the write operation. In
  // AUTO_FLUSH_BACKGROUND mode this method blocks awaiting enough space
  // in the mutation buffer to fit the specified write operation.
  Status CanApplyWriteOp(KuduWriteOperation* write_op,
                         KuduSession::FlushMode flush_mode,
                         bool* need_extra_flush,
                         int64_t* flush_watermark);

  // The client that this session is associated with.
  const sp::shared_ptr<KuduClient> client_;
  std::shared_ptr<rpc::Messenger> messenger_;

  // Lock protecting internal state.
  // Note that this lock should not be taken if the thread is already holding
  // a Batcher lock. This must be acquired first.
  mutable simple_spinlock lock_;

  // A dedicated lock to prevent multiple threads executing the ApplyWriteOp()
  // methods simultaneously. Should not be used for any other purpose.
  simple_spinlock apply_write_op_lock_;

  // Buffer for errors.
  scoped_refptr<internal::ErrorCollector> error_collector_;

  kudu::client::KuduSession::ExternalConsistencyMode external_consistency_mode_;

  // Timeout for the next batch.
  int timeout_ms_;

  // Interval for the max-wait flush background task.
  MonoDelta flush_interval_;  // protected by lock_

  // Whether the flush task is active/scheduled.
  bool flush_task_active_; // protected by lock_

  // Current flush mode for the incoming data.
  FlushMode flush_mode_;  // protected by lock_

  // The current batcher being prepared.
  scoped_refptr<internal::Batcher> batcher_;

  // Any batchers which have been flushed but not yet finished.
  //
  // Upon a batch finishing, it will call FlushFinished(), which removes the batcher from
  // this set. This set does not hold any reference count to the Batcher, since, while
  // the flush is active, the batcher manages its own refcount. The Batcher will always
  // call FlushFinished() before it destructs itself, so we're guaranteed that these
  // pointers stay valid.
  std::unordered_set<internal::Batcher*> flushed_batchers_;

  // Mutex for the flow_control_cond_ condition variable. This is to protect
  // variables related to byte counters, data flow control
  // and other variables which modification requries the thread which might
  // be waiting on the 'flow_control_cond_' condition variable to be notified.
  // This is different from the lock_: the lock_ is just to protect other
  // variables from simultaneous access. The current code does not try
  // to acquire both locks simulateneously.
  mutable Mutex flow_control_cond_mutex_;

  // Condition variable used by the data flow control while
  // applying/adding new write operations (based on cond_mutex_).
  ConditionVariable flow_control_cond_;

  // Session-wide limit on total size of buffer used by all batched write
  // operations. The buffer is a virtual entity: there isn't contiguous place
  // in the memory which would contain that 'buffed' data. Instead, buffer's
  // data is spread across all pending operations in all active batchers.
  size_t buffer_bytes_limit_; // protected by cond_mutex_

  // The high-watermark level as percentage of the buffer space used by
  // freshly added (not-yet-scheduled-for-flush) write operations.
  // Once the level is reached, the BackgroundFlusher triggers flushing
  // of accumulated write operations when running in AUTO_FLUSH_BACKGROUND mode.
  int32_t buffer_watermark_pct_;  // protected by cond_mutex_

  // The total number of bytes used by buffered write operations.
  int64_t total_bytes_used_;  // protected by cond_mutex_

  // Number of bytes consumed by buffered write operations
  // which are being flushed or scheduled to be flushed.
  int64_t flush_pending_bytes_; // protected by cond_mutex_

 private:
  FRIEND_TEST(ClientTest, TestAutoFlushBackgroundApplyBlocks);

  bool buffer_extra_flush_enabled_; // non-true value is used only in tests

  DISALLOW_COPY_AND_ASSIGN(Data);
};

} // namespace client
} // namespace kudu

#endif
