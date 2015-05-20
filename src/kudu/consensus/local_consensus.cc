// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/consensus/local_consensus.h"

#include <boost/thread/locks.hpp>
#include <iostream>

#include "kudu/consensus/log.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/server/metadata.h"
#include "kudu/server/clock.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/logging.h"
#include "kudu/util/trace.h"

namespace kudu {
namespace consensus {

using base::subtle::Barrier_AtomicIncrement;
using log::Log;
using log::LogEntryBatch;
using std::tr1::shared_ptr;
using strings::Substitute;

LocalConsensus::LocalConsensus(const ConsensusOptions& options,
                               gscoped_ptr<ConsensusMetadata> cmeta,
                               const string& peer_uuid,
                               const scoped_refptr<server::Clock>& clock,
                               ReplicaTransactionFactory* txn_factory,
                               Log* log,
                               const Closure& mark_dirty_clbk)
    : peer_uuid_(peer_uuid),
      options_(options),
      cmeta_(cmeta.Pass()),
      txn_factory_(DCHECK_NOTNULL(txn_factory)),
      log_(DCHECK_NOTNULL(log)),
      clock_(clock),
      mark_dirty_clbk_(mark_dirty_clbk),
      state_(kInitializing),
      next_op_id_index_(-1) {
  CHECK(cmeta_) << "Passed ConsensusMetadata object is NULL";
}

Status LocalConsensus::Start(const ConsensusBootstrapInfo& info) {
  TRACE_EVENT0("consensus", "LocalConsensus::Start");

  CHECK_EQ(state_, kInitializing);

  CHECK(info.orphaned_replicates.empty())
      << "LocalConsensus does not handle orphaned operations on start.";

  LOG_WITH_PREFIX(INFO) << "Starting LocalConsensus...";

  scoped_refptr<ConsensusRound> round;
  {
    boost::lock_guard<simple_spinlock> lock(lock_);

    const QuorumPB& quorum = cmeta_->committed_quorum();
    CHECK(quorum.local()) << "Local consensus must be passed a local quorum";
    RETURN_NOT_OK_PREPEND(VerifyQuorum(quorum, COMMITTED_QUORUM),
                          "Invalid quorum found in LocalConsensus::Start()");

    next_op_id_index_ = info.last_id.index() + 1;

    CHECK(quorum.peers(0).has_permanent_uuid()) << quorum.ShortDebugString();
    cmeta_->set_leader_uuid(quorum.peers(0).permanent_uuid());

    // TODO: This NO_OP is here mostly for unit tests. We should get rid of it.
    ReplicateMsg* replicate = new ReplicateMsg;
    replicate->set_op_type(NO_OP);
    NoOpRequestPB* noop_req = replicate->mutable_noop_request();
    noop_req->set_payload_for_tests("Starting up LocalConsensus");

    replicate->mutable_id()->set_term(0);
    replicate->mutable_id()->set_index(next_op_id_index_);
    replicate->set_timestamp(clock_->Now().ToUint64());

    round.reset(new ConsensusRound(this, make_scoped_refptr_replicate(replicate)));
    round->SetConsensusReplicatedCallback(
        Bind(&LocalConsensus::NoOpReplicationFinished,
             Unretained(this), Unretained(round.get())));
    state_ = kRunning;
  }

  Status s = Replicate(round);
  if (!s.ok()) {
    LOG_WITH_PREFIX(FATAL) << "Unable to replicate initial log entry: " << s.ToString();
  }

  TRACE("Consensus started");
  MarkDirty();
  return Status::OK();
}

bool LocalConsensus::IsRunning() const {
  boost::lock_guard<simple_spinlock> lock(lock_);
  return state_ == kRunning;
}

Status LocalConsensus::Replicate(const scoped_refptr<ConsensusRound>& round) {
  TRACE_EVENT0("consensus", "LocalConsensus::Replicate");
  DCHECK_GE(state_, kConfiguring);

  ReplicateMsg* msg = round->replicate_msg();

  OpId* cur_op_id = DCHECK_NOTNULL(msg)->mutable_id();
  cur_op_id->set_term(0);

  // Pre-cache the ByteSize outside of the lock, since this is somewhat
  // expensive.
  ignore_result(msg->ByteSize());

  LogEntryBatch* reserved_entry_batch;
  {
    boost::lock_guard<simple_spinlock> lock(lock_);

    // create the new op id for the entry.
    cur_op_id->set_index(next_op_id_index_++);
    // Reserve the correct slot in the log for the replication operation.
    // It's important that we do this under the same lock as we generate
    // the op id, so that we log things in-order.
    gscoped_ptr<log::LogEntryBatchPB> entry_batch;
    log::CreateBatchFromAllocatedOperations(
        { round->replicate_scoped_refptr() }, &entry_batch);

    RETURN_NOT_OK(log_->Reserve(log::REPLICATE, entry_batch.Pass(),
                                &reserved_entry_batch));

    // Local consensus transactions are always committed so we
    // can just persist the quorum, if this is a change config.
    if (round->replicate_msg()->op_type() == CHANGE_CONFIG_OP) {
      QuorumPB new_quorum = round->replicate_msg()->change_config_request().new_config();
      DCHECK(!new_quorum.has_opid_index());
      new_quorum.set_opid_index(round->replicate_msg()->id().index());
      cmeta_->set_committed_quorum(new_quorum);
      CHECK_OK(cmeta_->Flush());
    }
  }
  // Serialize and mark the message as ready to be appended.
  // When the Log actually fsync()s this message to disk, 'repl_callback'
  // is triggered.
  RETURN_NOT_OK(log_->AsyncAppend(
      reserved_entry_batch,
      Bind(&ConsensusRound::NotifyReplicationFinished, round)));
  return Status::OK();
}

QuorumPeerPB::Role LocalConsensus::role() const {
  return QuorumPeerPB::LEADER;
}

Status LocalConsensus::Update(const ConsensusRequestPB* request,
                              ConsensusResponsePB* response) {
  return Status::NotSupported("LocalConsensus does not support Update() calls.");
}

Status LocalConsensus::RequestVote(const VoteRequestPB* request,
                                   VoteResponsePB* response) {
  return Status::NotSupported("LocalConsensus does not support RequestVote() calls.");
}

void LocalConsensus::MarkDirty() {
  mark_dirty_clbk_.Run();
}

ConsensusStatePB LocalConsensus::CommittedConsensusState() const {
  boost::lock_guard<simple_spinlock> lock(lock_);
  return cmeta_->ToConsensusStatePB(ConsensusMetadata::COMMITTED);
}

void LocalConsensus::NoOpReplicationFinished(ConsensusRound* round, const Status& status) {
  CHECK_OK(status); // Replication should never fail for LocalConsensus.
  DCHECK_EQ(NO_OP, round->replicate_scoped_refptr()->get()->op_type());
  gscoped_ptr<CommitMsg> commit_msg(new CommitMsg);
  commit_msg->set_op_type(NO_OP);
  *commit_msg->mutable_commited_op_id() = round->id();
  WARN_NOT_OK(log_->AsyncAppendCommit(commit_msg.Pass(), Bind(&DoNothingStatusCB)),
              "Unable to append commit message");
}

QuorumPB LocalConsensus::CommittedQuorum() const {
  boost::lock_guard<simple_spinlock> lock(lock_);
  return cmeta_->committed_quorum();
}

void LocalConsensus::Shutdown() {
  VLOG_WITH_PREFIX(1) << "LocalConsensus Shutdown!";
}

void LocalConsensus::DumpStatusHtml(std::ostream& out) const {
  out << "<h1>Local Consensus Status</h1>\n";

  boost::lock_guard<simple_spinlock> lock(lock_);
  out << "next op: " << next_op_id_index_;
}

std::string LocalConsensus::LogPrefix() const {
  return Substitute("T $0 P $1: ", options_.tablet_id, peer_uuid_);
}

} // end namespace consensus
} // end namespace kudu
