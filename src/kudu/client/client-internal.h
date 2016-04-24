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
#ifndef KUDU_CLIENT_CLIENT_INTERNAL_H
#define KUDU_CLIENT_CLIENT_INTERNAL_H

#include <boost/function.hpp>
#include <set>
#include <string>
#include <unordered_set>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/util/atomic.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"

namespace kudu {

class DnsResolver;
class HostPort;

namespace master {
class AlterTableRequestPB;
class CreateTableRequestPB;
class GetLeaderMasterRpc;
class MasterServiceProxy;
} // namespace master

namespace rpc {
class Messenger;
class RpcController;
} // namespace rpc

namespace client {
namespace internal {

class Client : public std::enable_shared_from_this<Client> {
 public:
  Client();
  ~Client();

  // Selects a TS replica from the given RemoteTablet subject
  // to liveness and the provided selection criteria and blacklist.
  //
  // If no appropriate replica can be found, a non-OK status is returned and 'ts' is untouched.
  //
  // The 'candidates' return parameter indicates tservers that are live and meet the selection
  // criteria, but are possibly filtered by the blacklist. This is useful for implementing
  // retry logic.
  Status GetTabletServer(const scoped_refptr<internal::RemoteTablet>& rt,
                         KuduClient::ReplicaSelection selection,
                         const std::set<std::string>& blacklist,
                         std::vector<internal::RemoteTabletServer*>* candidates,
                         internal::RemoteTabletServer** ts);

  Status CreateTable(const master::CreateTableRequestPB& req,
                     const Schema& schema,
                     const MonoTime& deadline);

  Status OpenTable(string table_name,
                   const MonoTime& deadline,
                   std::shared_ptr<Table>* table);

  Status IsCreateTableInProgress(const std::string& table_name,
                                 const MonoTime& deadline,
                                 bool *create_in_progress);

  Status WaitForCreateTableToFinish(const std::string& table_name,
                                    const MonoTime& deadline);

  Status DeleteTable(const std::string& table_name,
                     const MonoTime& deadline);

  Status AlterTable(const master::AlterTableRequestPB& req,
                    const MonoTime& deadline);

  Status IsAlterTableInProgress(const std::string& table_name,
                                const MonoTime& deadline,
                                bool *alter_in_progress);

  Status WaitForAlterTableToFinish(const std::string& alter_name,
                                   const MonoTime& deadline);

  Status GetTableSchema(const std::string& table_name,
                        const MonoTime& deadline,
                        Schema* schema,
                        PartitionSchema* partition_schema,
                        std::string* table_id);

  Status ListTabletServers(std::vector<KuduTabletServer*>* tablet_servers,
                           const MonoTime& deadline);

  Status ListTables(std::vector<std::string>* tables,
                    const std::string& filter,
                    const MonoTime& deadline);

  Status InitLocalHostNames();

  bool IsLocalHostPort(const HostPort& hp) const;

  bool IsTabletServerLocal(const internal::RemoteTabletServer& rts) const;

  // Returns a non-failed replica of the specified tablet based on the provided selection criteria
  // and tablet server blacklist.
  //
  // Returns NULL if there are no valid tablet servers.
  internal::RemoteTabletServer* SelectTServer(
      const scoped_refptr<internal::RemoteTablet>& rt,
      const KuduClient::ReplicaSelection selection,
      const std::set<std::string>& blacklist,
      std::vector<internal::RemoteTabletServer*>* candidates) const;

  // Sets 'master_proxy_' from the address specified by
  // 'leader_master_hostport_'.  Called by
  // GetLeaderMasterRpc::SendRpcCb() upon successful completion.
  //
  // See also: SetMasterServerProxyAsync.
  void LeaderMasterDetermined(const Status& status,
                              const HostPort& host_port);

  // Asynchronously sets 'master_proxy_' to the leader master by
  // cycling through servers listed in 'master_server_addrs_' until
  // one responds with a Raft configuration that contains the leader
  // master or 'deadline' expires.
  //
  // Invokes 'cb' with the appropriate status when finished.
  //
  // Works with both a distributed and non-distributed configuration.
  void SetMasterServerProxyAsync(const MonoTime& deadline,
                                 const StatusCallback& cb);

  // Synchronous version of SetMasterServerProxyAsync method above.
  //
  // NOTE: since this uses a Synchronizer, this may not be invoked by
  // a method that's on a reactor thread.
  //
  // TODO (KUDU-492): Get rid of this method and re-factor the client
  // to lazily initialize 'master_proxy_'.
  Status SetMasterServerProxy(const MonoTime& deadline);

  std::shared_ptr<master::MasterServiceProxy> master_proxy() const;

  HostPort leader_master_hostport() const;

  uint64_t GetLatestObservedTimestamp() const;

  void UpdateLatestObservedTimestamp(uint64_t timestamp);

  // Retry 'func' until either:
  //
  // 1) Methods succeeds on a leader master.
  // 2) Method fails for a reason that is not related to network
  //    errors, timeouts, or leadership issues.
  // 3) 'deadline' (if initialized) elapses.
  //
  // If 'num_attempts' is not NULL, it will be incremented on every
  // attempt (successful or not) to call 'func'.
  //
  // NOTE: 'rpc_timeout' is a per-call timeout, while 'deadline' is a
  // per operation deadline. If 'deadline' is not initialized, 'func' is
  // retried forever. If 'deadline' expires, 'func_name' is included in
  // the resulting Status.
  template<class ReqClass, class RespClass>
  Status SyncLeaderMasterRpc(
      const MonoTime& deadline,
      Client* client,
      const ReqClass& req,
      RespClass* resp,
      int* num_attempts,
      const char* func_name,
      const boost::function<Status(master::MasterServiceProxy*,
                                   const ReqClass&, RespClass*,
                                   rpc::RpcController*)>& func);

  bool IsMultiMaster() const {
    return master_server_addrs_.size() > 1;
  }

  const MonoDelta& default_admin_operation_timeout() const {
    return default_admin_operation_timeout_;
  }

  const MonoDelta& default_rpc_timeout() const {
    return default_rpc_timeout_;
  }

  std::shared_ptr<rpc::Messenger> messenger_;
  gscoped_ptr<DnsResolver> dns_resolver_;
  scoped_refptr<internal::MetaCache> meta_cache_;

  // Set of hostnames and IPs on the local host.
  // This is initialized at client startup.
  std::unordered_set<std::string> local_host_names_;

  // Options the client was built with.
  std::vector<std::string> master_server_addrs_;
  MonoDelta default_admin_operation_timeout_;
  MonoDelta default_rpc_timeout_;

  // The host port of the leader master. This is set in
  // LeaderMasterDetermined, which is invoked as a callback by
  // SetMasterServerProxyAsync.
  HostPort leader_master_hostport_;

  // Proxy to the leader master.
  std::shared_ptr<master::MasterServiceProxy> master_proxy_;

  // Ref-counted RPC instance: since 'SetMasterServerProxyAsync' call
  // is asynchronous, we need to hold a reference in this class
  // itself, as to avoid a "use-after-free" scenario.
  scoped_refptr<master::GetLeaderMasterRpc> leader_master_rpc_;
  std::vector<StatusCallback> leader_master_callbacks_;

  // Protects 'leader_master_rpc_', 'leader_master_hostport_',
  // and master_proxy_
  //
  // See: Client::SetMasterServerProxyAsync for a more
  // in-depth explanation of why this is needed and how it works.
  mutable simple_spinlock leader_master_lock_;

  AtomicInt<uint64_t> latest_observed_timestamp_;

  DISALLOW_COPY_AND_ASSIGN(Client);
};

// Retry helper, takes a function like: Status funcName(const MonoTime& deadline, bool *retry, ...)
// The function should set the retry flag (default true) if the function should
// be retried again. On retry == false the return status of the function will be
// returned to the caller, otherwise a Status::Timeout() will be returned.
// If the deadline is already expired, no attempt will be made.
Status RetryFunc(const MonoTime& deadline,
                 const std::string& retry_msg,
                 const std::string& timeout_msg,
                 const boost::function<Status(const MonoTime&, bool*)>& func);

} // namespace internal
} // namespace client
} // namespace kudu

#endif
