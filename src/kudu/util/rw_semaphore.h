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
#ifndef KUDU_UTIL_RW_SEMAPHORE_H
#define KUDU_UTIL_RW_SEMAPHORE_H

#include <atomic>
#include <glog/logging.h>
#include <thread>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/util/debug-util.h"

#include "kudu/util/thread.h"

namespace kudu {

// Read-Write semaphore. 32bit uint that contains the number of readers.
// When someone wants to write, tries to set the 32bit, and waits until
// the readers have finished. Readers are spinning while the write flag is set.
//
// This rw-semaphore makes no attempt at fairness, though it does avoid write
// starvation (no new readers may obtain the lock if a write is waiting).
//
// Given that this is currently based only on spinning (and not futex),
// it should only be used in cases where the lock is held for very short
// time intervals.
//
// If the semaphore is expected to always be released from the same thread
// that acquired it, use rw_spinlock instead.
//
// In order to support easier debugging of leaked locks, this class can track
// the stack trace of the last thread to lock it in write mode. To do so,
// uncomment the following define:
//   #define RW_SEMAPHORE_TRACK_HOLDER 1
// ... and then in gdb, print the contents of the semaphore, and you should
// see the collected stack trace.
class rw_semaphore {
 public:
  rw_semaphore() : state_(0) {
  }
  ~rw_semaphore() {}

  void lock_shared() {
    uint32_t cur_state = state_.load(std::memory_order_relaxed);
    while (true) {
      cur_state &= kNumReadersMask;             // I expect no writer lock
      uint32_t try_new_state = cur_state + 1;   // Add me as reader
      if (state_.compare_exchange_weak(cur_state, try_new_state, std::memory_order_acquire)) {
        break;
      }
      // Either was already locked by someone else, or CAS failed.
      std::this_thread::yield();
    }
  }

  void unlock_shared() {
    uint32_t cur_state = state_.load(std::memory_order_relaxed);
    while (true) {
      DCHECK_GT(cur_state & kNumReadersMask, 0)
        << "unlock_shared() called when there are no shared locks held";
      uint32_t try_new_state = cur_state - 1;   // Drop me as reader
      if (state_.compare_exchange_weak(cur_state, try_new_state, std::memory_order_release)) {
        break;
      }
      // Either was already locked by someone else, or CAS failed.
      std::this_thread::yield();
    }
  }

  // Tries to acquire a write lock, if no one else has it.
  // This function retries on CAS failure and waits for readers to complete.
  bool try_lock() {
    uint32_t cur_state = state_.load(std::memory_order_relaxed);
    while (true) {
      // someone else has already the write lock
      if (cur_state & kWriteFlag)
        return false;

      cur_state &= kNumReadersMask;                     // I expect some 0+ readers
      uint32_t try_new_state = kWriteFlag | cur_state;  // I want to lock the other writers
      if (state_.compare_exchange_weak(cur_state, try_new_state, std::memory_order_acquire)) {
        break;
      }
      // Either was already locked by someone else, or CAS failed.
      std::this_thread::yield();
    }

    WaitPendingReaders();
    RecordLockHolderStack();
    return true;
  }

  void lock() {
    uint32_t cur_state = state_.load(std::memory_order_relaxed);
    while (true) {
      cur_state &= kNumReadersMask;                     // I expect some 0+ readers
      Atomic32 try_new_state = kWriteFlag | cur_state;  // I want to lock the other writers
      // Note: we use NoBarrier here because we'll do the Acquire barrier down below
      // in WaitPendingReaders
      if (state_.compare_exchange_weak(cur_state, try_new_state, std::memory_order_relaxed)) {
        break;
      }
      // Either was already locked by someone else, or CAS failed.
      std::this_thread::yield();
    }

    WaitPendingReaders();

#ifndef NDEBUG
    writer_tid_ = Thread::CurrentThreadId();
#endif // NDEBUG
    RecordLockHolderStack();
  }

  void unlock() {
    // I expect to be the only writer
    DCHECK_EQ(state_.load(std::memory_order_relaxed), kWriteFlag);

#ifndef NDEBUG
    writer_tid_ = -1; // Invalid tid.
#endif // NDEBUG

    ResetLockHolderStack();
    // Reset: no writers & no readers.
    state_.store(0, std::memory_order_release);
  }

  // Return true if the lock is currently held for write by any thread.
  // See simple_semaphore::is_locked() for details about where this is useful.
  bool is_write_locked() const {
    return state_.load(std::memory_order_relaxed) & kWriteFlag;
  }

  // Return true if the lock is currently held, either for read or write
  // by any thread.
  // See simple_semaphore::is_locked() for details about where this is useful.
  bool is_locked() const {
    return state_.load(std::memory_order_relaxed);
  }

 private:
  static const uint32_t kNumReadersMask = 0x7fffffff;
  static const uint32_t kWriteFlag = 1 << 31;

#ifdef RW_SEMAPHORE_TRACK_HOLDER
  StackTrace writer_stack_;
  void RecordLockHolderStack() {
    writer_stack_.Collect();
  }
  void ResetLockHolderStack() {
    writer_stack_.Reset();
  }
#else
  void RecordLockHolderStack() {
  }
  void ResetLockHolderStack() {
  }
#endif

  void WaitPendingReaders() {
    while ((state_.load(std::memory_order_acquire) & kNumReadersMask) > 0) {
      std::this_thread::yield();
    }
  }

 private:
  volatile std::atomic<uint32_t> state_;
#ifndef NDEBUG
  int64_t writer_tid_;
#endif // NDEBUG
};

} // namespace kudu
#endif /* KUDU_UTIL_RW_SEMAPHORE_H */
