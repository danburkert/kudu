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

#include <atomic>
#include <boost/thread/thread.hpp>
#include <memory>

#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/thread.h"
#include "kudu/tablet/diskrowset-test-base.h"

enum {
  kDefaultNumSecondsPerThread = 1,
  kDefaultNumFlushThreads = 4,
  kDefaultNumCompactionThreads = 4,
};

DEFINE_int32(num_update_threads, 1, "Number of updater threads");
DEFINE_int32(num_flush_threads, kDefaultNumFlushThreads, "Number of flusher threads");
DEFINE_int32(num_compaction_threads, kDefaultNumCompactionThreads, "Number of compaction threads");
DEFINE_int32(num_seconds_per_thread, kDefaultNumSecondsPerThread,
             "Minimum number of seconds each thread should work");

using std::atomic;
using std::memory_order_relaxed;
using std::shared_ptr;

namespace kudu {
namespace tablet {

class TestMultiThreadedRowSetDeltaCompaction : public TestRowSet {
 public:

  TestMultiThreadedRowSetDeltaCompaction()
      : TestRowSet(),
        update_counter_(0),
        should_run_(true) {
  }

  // This thread read the value of an atomic integer, updates all rows
  // in 'rs' to the value + 1, and then sets the atomic integer back
  // to value + 1. This is done so that the verifying threads knows the
  // latest expected value of the row (simply calling AtomicIncrement
  // won't work as a thread setting a value n+1 is not guaranteed to finish
  // before a thread setting value n).
  void RowSetUpdateThread(DiskRowSet *rs) {
    while (ShouldRun()) {
      uint32_t val = update_counter_.load();
      UpdateRowSet(rs, val + 1);
      if (ShouldRun()) {
        update_counter_.store(val + 1);
      }
    }
  }

  void RowSetFlushThread(DiskRowSet *rs) {
    while (ShouldRun()) {
      if (rs->CountDeltaStores() < 5) {
        CHECK_OK(rs->FlushDeltas());
      } else {
        SleepFor(MonoDelta::FromMilliseconds(10));
      }
    }
  }

  void RowSetDeltaCompactionThread(DiskRowSet *rs) {
    while (ShouldRun()) {
      CHECK_OK(rs->MinorCompactDeltaStores());
    }
  }

  void ReadVerify(DiskRowSet *rs) {
    Arena arena(1024, 1024*1024);
    RowBlock dst(schema_, 1000, &arena);
    gscoped_ptr<RowwiseIterator> iter;
    ASSERT_OK(rs->NewRowIterator(&schema_,
                                 MvccSnapshot::CreateSnapshotIncludingAllTransactions(),
                                 &iter));
    uint32_t expected = update_counter_.load(memory_order_relaxed);
    ASSERT_OK(iter->Init(nullptr));
    while (iter->HasNext()) {
      ASSERT_OK_FAST(iter->NextBlock(&dst));
      size_t n = dst.nrows();
      ASSERT_GT(n, 0);
      for (size_t j = 0; j < n; j++) {
        uint32_t val = *schema_.ExtractColumnFromRow<UINT32>(dst.row(j), 1);
        ASSERT_GE(val, expected);
      }
    }
  }

  void StartThreads(DiskRowSet *rs) {
    for (int i = 0; i < FLAGS_num_update_threads; i++) {
      scoped_refptr<kudu::Thread> thread;
      CHECK_OK(kudu::Thread::Create("test", strings::Substitute("log_writer$0", i),
          &TestMultiThreadedRowSetDeltaCompaction::RowSetUpdateThread, this, rs, &thread));
      update_threads_.push_back(thread);
    }
    for (int i = 0; i < FLAGS_num_flush_threads; i++) {
      scoped_refptr<kudu::Thread> thread;
      CHECK_OK(kudu::Thread::Create("test", strings::Substitute("delta_flush$0", i),
          &TestMultiThreadedRowSetDeltaCompaction::RowSetFlushThread, this, rs, &thread));
      flush_threads_.push_back(thread);
    }
    for (int i = 0; i < FLAGS_num_compaction_threads; i++) {
      scoped_refptr<kudu::Thread> thread;
      CHECK_OK(kudu::Thread::Create("test", strings::Substitute("delta_compaction$0", i),
          &TestMultiThreadedRowSetDeltaCompaction::RowSetDeltaCompactionThread, this, rs, &thread));
      compaction_threads_.push_back(thread);
    }
  }

  void JoinThreads() {
    for (int i = 0; i < update_threads_.size(); i++) {
      ASSERT_OK(ThreadJoiner(update_threads_[i].get()).Join());
    }
    for (int i = 0; i < flush_threads_.size(); i++) {
      ASSERT_OK(ThreadJoiner(flush_threads_[i].get()).Join());
    }
    for (int i = 0; i < compaction_threads_.size(); i++) {
      ASSERT_OK(ThreadJoiner(compaction_threads_[i].get()).Join());
    }
    for (int i = 0; i < alter_schema_threads_.size(); i++) {
      ASSERT_OK(ThreadJoiner(alter_schema_threads_[i].get()).Join());
    }
  }

  void WriteTestRowSetWithZeros() {
    WriteTestRowSet(0, true);
  }

  void UpdateRowSet(DiskRowSet *rs, uint32_t value) {
    for (uint32_t idx = 0; idx < n_rows_ && ShouldRun(); idx++) {
      OperationResultPB result;
      ASSERT_OK_FAST(UpdateRow(rs, idx, value, &result));
    }
  }

  void TestUpdateAndVerify() {
    WriteTestRowSetWithZeros();
    shared_ptr<DiskRowSet> rs;
    ASSERT_OK(OpenTestRowSet(&rs));

    StartThreads(rs.get());
    SleepFor(MonoDelta::FromSeconds(FLAGS_num_seconds_per_thread));
    should_run_.store(false, memory_order_relaxed);
    ASSERT_NO_FATAL_FAILURE(JoinThreads());

    ASSERT_NO_FATAL_FAILURE(ReadVerify(rs.get()));
  }

  bool ShouldRun() const {
    return should_run_.load(memory_order_relaxed);
  }

 protected:

  atomic<uint32_t> update_counter_;
  // Signal to threads to keep running. No ordering or visibility is implied by
  // accessing the variable, so relaxed operations are used.
  atomic<bool> should_run_;
  vector<scoped_refptr<kudu::Thread> > update_threads_;
  vector<scoped_refptr<kudu::Thread> > flush_threads_;
  vector<scoped_refptr<kudu::Thread> > compaction_threads_;
  vector<scoped_refptr<kudu::Thread> > alter_schema_threads_;
};

static void SetupFlagsForSlowTests() {
  if (kDefaultNumSecondsPerThread == FLAGS_num_seconds_per_thread) {
    FLAGS_num_seconds_per_thread = 40;
  }
  if (kDefaultNumFlushThreads == FLAGS_num_flush_threads) {
    FLAGS_num_flush_threads = 8;
  }
  if (kDefaultNumCompactionThreads == FLAGS_num_compaction_threads) {
    FLAGS_num_compaction_threads = 8;
  }
}

TEST_F(TestMultiThreadedRowSetDeltaCompaction, TestMTUpdateAndCompact) {
  if (AllowSlowTests()) {
    SetupFlagsForSlowTests();
  }

  TestUpdateAndVerify();
}

} // namespace tablet
} // namespace kudu
