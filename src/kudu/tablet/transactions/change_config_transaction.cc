// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/tablet/transactions/change_config_transaction.h"

#include "kudu/common/wire_protocol.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/server/hybrid_clock.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_peer.h"
#include "kudu/tablet/tablet_metrics.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/semaphore.h"

namespace kudu {
namespace tablet {

using boost::bind;
using boost::shared_lock;
using consensus::ReplicateMsg;
using consensus::CommitMsg;
using consensus::OP_ABORT;
using consensus::CHANGE_CONFIG_OP;
using consensus::DriverType;
using metadata::QuorumPB;
using strings::Substitute;
using tserver::TabletServerErrorPB;

string ChangeConfigTransactionState::ToString() const {
  return Substitute("ChangeConfigTransactionState [timestamp=$0, request=$1]",
                    has_timestamp() ? timestamp().ToString() : "NULL",
                    request_ == NULL ? "(none)" : request_->ShortDebugString());
}

ChangeConfigTransaction::ChangeConfigTransaction(ChangeConfigTransactionState* tx_state,
                                                 DriverType type,
                                                 Semaphore* config_sem)
    : Transaction(tx_state, type, Transaction::CHANGE_CONFIG_TXN),
      state_(tx_state),
      config_sem_(config_sem) {
}

void ChangeConfigTransaction::NewReplicateMsg(gscoped_ptr<ReplicateMsg>* replicate_msg) {
  replicate_msg->reset(new ReplicateMsg);
  (*replicate_msg)->set_op_type(CHANGE_CONFIG_OP);
  (*replicate_msg)->mutable_change_config_request()->CopyFrom(*state()->request());
}

Status ChangeConfigTransaction::Prepare() {
  TRACE("PREPARE CHANGE CONFIG: Starting");

  state_->acquire_config_sem(config_sem_);

  DCHECK(state_->old_quorum().IsInitialized());
  DCHECK(state_->request()->has_new_config());
  const QuorumPB& old_quorum = state_->old_quorum();
  const QuorumPB& new_quorum = state_->request()->new_config();

  Status s;
  if (old_quorum.seqno() >= new_quorum.seqno()) {
    s = Status::IllegalState(Substitute("New Quorum configuration has a "
        "lower sequence number than the old configuration. Old: $0. New: $1",
        old_quorum.DebugString(), new_quorum.DebugString()));
  }

  TRACE("PREPARE CHANGE CONFIG: finished (Status: $0)", s.ToString());

  if (!s.ok()) {
    state_->completion_callback()->set_error(s, TabletServerErrorPB::INVALID_CONFIG);
  }

  return s;
}

Status ChangeConfigTransaction::Start() {
  // now that we've acquired the semaphore, set the transaction timestamp
  state_->set_timestamp(state_->tablet_peer()->clock()->Now());
#ifdef __linux__
  TRACE("START. Timestamp: $0", server::HybridClock::GetPhysicalValue(state_->timestamp()));
#endif
  return Status::OK();
}

void ChangeConfigTransaction::NewCommitAbortMessage(gscoped_ptr<CommitMsg>* commit_msg) {
  commit_msg->reset(new CommitMsg());
  (*commit_msg)->set_op_type(OP_ABORT);
  (*commit_msg)->mutable_change_config_response()->CopyFrom(*state_->response());
}

Status ChangeConfigTransaction::Apply(gscoped_ptr<CommitMsg>* commit_msg) {
  TRACE("APPLY CHANGE CONFIG: Starting");

  commit_msg->reset(new CommitMsg());
  (*commit_msg)->set_op_type(CHANGE_CONFIG_OP);
  (*commit_msg)->set_timestamp(state_->timestamp().ToUint64());
  return Status::OK();
}

void ChangeConfigTransaction::Finish() {
  // Now that all of the changes have been applied and the commit is durable
  // make the changes visible to readers.
  TRACE("APPLY CHANGE CONFIG: apply finished");

  // Notify the peer that the consensus state has changed.
  state_->tablet_peer()->ConsensusStateChanged(state_->old_quorum(),
                                               state_->request()->new_config());
  state()->commit();
}

string ChangeConfigTransaction::ToString() const {
  return Substitute("ChangeConfigTransaction [type=$0, state=$1]",
                    DriverType_Name(type()),
                    state_->ToString());
}

}  // namespace tablet
}  // namespace kudu
