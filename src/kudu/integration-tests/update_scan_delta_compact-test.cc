// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/client/client.h"
#include "kudu/client/row_result.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/integration-tests/mini_cluster.h"
#include "kudu/master/mini_master.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/monotime.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"

DECLARE_int32(flush_threshold_mb);
DECLARE_int32(log_segment_size_mb);
DECLARE_int32(maintenance_manager_polling_interval_ms);
DEFINE_int32(mbs_for_flushes_and_rolls, 1, "How many MBs are needed to flush and roll");
DEFINE_int32(row_count, 2000, "How many rows will be used in this test for the base data");
DEFINE_int32(seconds_to_run, 4,
             "How long this test runs for, after inserting the base data, in seconds");

using std::tr1::shared_ptr;

namespace kudu {
namespace tablet {

using client::KuduInsert;
using client::KuduClient;
using client::KuduClientBuilder;
using client::KuduColumnSchema;
using client::KuduRowResult;
using client::KuduScanner;
using client::KuduSchema;
using client::KuduSession;
using client::KuduTable;
using client::KuduUpdate;

// This integration test tries to trigger all the update-related bits while also serving as a
// foundation for benchmarking. It first inserts 'row_count' rows and then starts two threads,
// one that continuously updates all the rows sequentially and one that scans them all, until
// it's been running for 'seconds_to_run'. It doesn't test for correctness, unless something
// FATALs.
class UpdateScanDeltaCompactionTest : public KuduTest {
 protected:
  UpdateScanDeltaCompactionTest()
      :
      schema_({ KuduColumnSchema("key", KuduColumnSchema::INT64),
                KuduColumnSchema("string_val", KuduColumnSchema::STRING),
                KuduColumnSchema("int64", KuduColumnSchema::INT64) }, 1) {
  }

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();
  }

  void CreateTable() {
    ASSERT_NO_FATAL_FAILURE(InitCluster());
    ASSERT_OK(client_->NewTableCreator()
             ->table_name(kTableName)
             .schema(&schema_)
             .Create());
    ASSERT_OK(client_->OpenTable(kTableName, &table_));
  }

  virtual void TearDown() OVERRIDE {
    if (cluster_) {
      cluster_->Shutdown();
    }
    KuduTest::TearDown();
  }

  // Inserts row_count rows sequentially.
  void InsertBaseData();

  // Starts the update and scan threads then stops them after seconds_to_run.
  void RunUpdateAndScanThreads();

 private:
  enum {
    kKeyCol,
    kStrCol,
    kInt64Col
  };
  static const char* const kTableName;

  void InitCluster() {
    // Start mini-cluster with 1 tserver.
    cluster_.reset(new MiniCluster(env_.get(), MiniClusterOptions()));
    ASSERT_OK(cluster_->Start());
    KuduClientBuilder client_builder;
    client_builder.add_master_server_addr(
        cluster_->mini_master()->bound_rpc_addr_str());
    ASSERT_OK(client_builder.Build(&client_));
  }

  shared_ptr<KuduSession> CreateSession() {
    shared_ptr<KuduSession> session = client_->NewSession();
    session->SetTimeoutMillis(5000);
    CHECK_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    return session;
  }

  // Continuously updates the existing data until 'stop_latch' drops to 0.
  void UpdateRows(CountDownLatch* stop_latch);

  // Continuously scans the data until 'stop_latch' drops to 0.
  void ScanRows(CountDownLatch* stop_latch) const;

  // Sets the passed values on the row.
  // TODO randomize the string column.
  void MakeRow(int64_t key, int64_t val, KuduPartialRow* row) const;

  // If 'key' is a multiple of kSessionBatchSize, it uses 'last_s' to wait for the previous batch
  // to finish and then flushes the current one.
  Status WaitForLastBatchAndFlush(int64_t key,
                                  Synchronizer* last_s,
                                  shared_ptr<KuduSession> session);

  KuduSchema schema_;
  shared_ptr<MiniCluster> cluster_;
  scoped_refptr<KuduTable> table_;
  shared_ptr<KuduClient> client_;
};

const char* const UpdateScanDeltaCompactionTest::kTableName = "update-scan-delta-compact-tbl";
const int kSessionBatchSize = 1000;

TEST_F(UpdateScanDeltaCompactionTest, TestAll) {
  OverrideFlagForSlowTests("seconds_to_run", "100");
  OverrideFlagForSlowTests("row_count", "1000000");
  OverrideFlagForSlowTests("mbs_for_flushes_and_rolls", "8");
  // Setting this high enough that we see the effects of flushes and compactions.
  OverrideFlagForSlowTests("maintenance_manager_polling_interval_ms", "2000");
  FLAGS_flush_threshold_mb = FLAGS_mbs_for_flushes_and_rolls;
  FLAGS_log_segment_size_mb = FLAGS_mbs_for_flushes_and_rolls;
  if (!AllowSlowTests()) {
    // Make it run more often since it's not a long test.
    FLAGS_maintenance_manager_polling_interval_ms = 50;
  }

  ASSERT_NO_FATAL_FAILURE(CreateTable());
  ASSERT_NO_FATAL_FAILURE(InsertBaseData());
  ASSERT_NO_FATAL_FAILURE(RunUpdateAndScanThreads());
}

void UpdateScanDeltaCompactionTest::InsertBaseData() {
  shared_ptr<KuduSession> session = CreateSession();
  Synchronizer last_s;
  last_s.StatusCB(Status::OK());

  LOG_TIMING(INFO, "Insert") {
    for (int64_t key = 0; key < FLAGS_row_count; key++) {
      gscoped_ptr<KuduInsert> insert = table_->NewInsert();
      MakeRow(key, 0, insert->mutable_row());
      ASSERT_OK(session->Apply(insert.Pass()));
      ASSERT_OK(WaitForLastBatchAndFlush(key, &last_s, session));
    }
    ASSERT_OK(WaitForLastBatchAndFlush(kSessionBatchSize, &last_s, session));
    ASSERT_OK(last_s.Wait());
  }
}

void UpdateScanDeltaCompactionTest::RunUpdateAndScanThreads() {
  scoped_refptr<Thread> update_thread;
  scoped_refptr<Thread> scan_thread;
  CountDownLatch stop_latch(1);
  ASSERT_OK(Thread::Create(CURRENT_TEST_NAME(),
                           StrCat(CURRENT_TEST_CASE_NAME(), "-update"),
                           &UpdateScanDeltaCompactionTest::UpdateRows, this,
                           &stop_latch, &update_thread));

  ASSERT_OK(Thread::Create(CURRENT_TEST_NAME(),
                           StrCat(CURRENT_TEST_CASE_NAME(), "-scan"),
                           &UpdateScanDeltaCompactionTest::ScanRows, this,
                           &stop_latch, &scan_thread));
  SleepFor(MonoDelta::FromSeconds(FLAGS_seconds_to_run * 1.0));
  stop_latch.CountDown();
  ASSERT_OK(ThreadJoiner(update_thread.get())
            .warn_every_ms(500)
            .Join());
  ASSERT_OK(ThreadJoiner(scan_thread.get())
            .warn_every_ms(500)
            .Join());
}

void UpdateScanDeltaCompactionTest::UpdateRows(CountDownLatch* stop_latch) {
  shared_ptr<KuduSession> session = CreateSession();
  Synchronizer last_s;

  for (int64_t iteration = 1; stop_latch->count() > 0; iteration++) {
    last_s.StatusCB(Status::OK());
    LOG_TIMING(INFO, "Update") {
      for (int64_t key = 0; key < FLAGS_row_count && stop_latch->count() > 0; key++) {
        gscoped_ptr<KuduUpdate> update = table_->NewUpdate();
        MakeRow(key, iteration, update->mutable_row());
        CHECK_OK(session->Apply(update.Pass()));
        CHECK_OK(WaitForLastBatchAndFlush(key, &last_s, session));
      }
      CHECK_OK(WaitForLastBatchAndFlush(kSessionBatchSize, &last_s, session));
      CHECK_OK(last_s.Wait());
    }
  }
}

void UpdateScanDeltaCompactionTest::ScanRows(CountDownLatch* stop_latch) const {
  while (stop_latch->count() > 0) {
    KuduScanner scanner(table_.get());
    CHECK_OK(scanner.SetProjection(&schema_));
    LOG_TIMING(INFO, "Scan") {
      CHECK_OK(scanner.Open());
      vector<KuduRowResult> rows;
      while (scanner.HasMoreRows()) {
        CHECK_OK(scanner.NextBatch(&rows));
        rows.clear();
      }
    }
  }
}

void UpdateScanDeltaCompactionTest::MakeRow(int64_t key,
                                            int64_t val,
                                            KuduPartialRow* row) const {
  CHECK_OK(row->SetInt64(kKeyCol, key));
  CHECK_OK(row->SetStringCopy(kStrCol, "TODO random string"));
  CHECK_OK(row->SetInt64(kInt64Col, val));
}

Status UpdateScanDeltaCompactionTest::WaitForLastBatchAndFlush(int64_t key,
                                                               Synchronizer* last_s,
                                                               shared_ptr<KuduSession> session) {
  if (key % kSessionBatchSize == 0) {
    RETURN_NOT_OK(last_s->Wait());
    last_s->Reset();
    session->FlushAsync(last_s->AsStatusCallback());
  }
  return Status::OK();
}

} // namespace tablet
} // namespace kudu
