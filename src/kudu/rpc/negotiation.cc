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

#include "kudu/rpc/negotiation.h"

#include <sys/time.h>
#include <poll.h>

#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/blocking_ops.h"
#include "kudu/rpc/connection.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/reactor.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/sasl_client.h"
#include "kudu/rpc/sasl_common.h"
#include "kudu/rpc/sasl_server.h"
#include "kudu/rpc/serialization.h"
#include "kudu/util/errno.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/status.h"
#include "kudu/util/trace.h"

DEFINE_bool(rpc_trace_negotiation, false,
            "If enabled, dump traces of all RPC negotiations to the log");
TAG_FLAG(rpc_trace_negotiation, runtime);
TAG_FLAG(rpc_trace_negotiation, advanced);
TAG_FLAG(rpc_trace_negotiation, experimental);

DEFINE_int32(rpc_negotiation_inject_delay_ms, 0,
             "If enabled, injects the given number of milliseconds delay into "
             "the RPC negotiation process on the server side.");
TAG_FLAG(rpc_negotiation_inject_delay_ms, unsafe);

using std::shared_ptr;
using strings::Substitute;

namespace kudu {
namespace rpc {

// Client: Send ConnectionContextPB message based on information stored in the Connection object.
static Status SendConnectionContext(Connection* conn, const MonoTime& deadline) {
  TRACE("Sending connection context");
  RequestHeader header;
  header.set_call_id(kConnectionContextCallId);

  ConnectionContextPB conn_context;
  // This field is deprecated but used by servers <Kudu 1.1. Newer server versions ignore
  // this and use the SASL-provided username instead.
  conn_context.mutable_deprecated_user_info()->set_real_user(conn->user_credentials().real_user());
  return SendFramedMessageBlocking(conn->socket(), header, conn_context, deadline);
}

// Server: Receive ConnectionContextPB message and update the corresponding fields in the
// associated Connection object. Perform validation against SASL-negotiated information
// as needed.
static Status RecvConnectionContext(Connection* conn, const MonoTime& deadline) {
  TRACE("Waiting for connection context");
  faststring recv_buf(1024); // Should be plenty for a ConnectionContextPB message.
  RequestHeader header;
  Slice param_buf;
  RETURN_NOT_OK(ReceiveFramedMessageBlocking(conn->socket(), &recv_buf,
                                             &header, &param_buf, deadline));
  DCHECK(header.IsInitialized());

  if (header.call_id() != kConnectionContextCallId) {
    return Status::IllegalState("Expected ConnectionContext callid, received",
                                std::to_string(header.call_id()));
  }

  ConnectionContextPB conn_context;
  if (!conn_context.ParseFromArray(param_buf.data(), param_buf.size())) {
    return Status::InvalidArgument("Received invalid ConnectionContextPB message, missing fields",
                                   conn_context.InitializationErrorString());
  }

  if (conn->sasl_server().authenticated_user().empty()) {
    return Status::NotAuthorized("No user was authenticated");
  }

  conn->mutable_user_credentials()->set_real_user(conn->sasl_server().authenticated_user());

  return Status::OK();
}

// Reads a NegotiatePB from the connection, filling in the header and the
// message. Checks the header's call id matches the expected negotiation call
// id.
//
// Parameters:
//  * conn - the connection.
//  * tag - the negotiation step, for debugging and tracing purposes.
//  * buffer - a scratch buffer.
//  * deadline - the negotiation deadline.
//  * header - the request or response header.
//  * msg - the negotiate protobuf message.
template<typename Header>
static Status RecvNegotiationMsg(Connection* conn,
                               const string& tag,
                               faststring* buffer,
                               const MonoTime& deadline,
                               Header* header,
                               NegotiatePB* msg) {
  // Directional string literals for debugging.
  auto direction = [conn] () {
    return conn->direction() == Connection::SERVER ? "Server" : "Client";
  };
  auto msg_type = [conn] () {
    return conn->direction() == Connection::SERVER ? "request" : "response";
  };

  TRACE("$0: Waiting for $1 $2...", direction(), tag, msg_type());
  Slice msg_buf;
  RETURN_NOT_OK(ReceiveFramedMessageBlocking(conn->socket(), buffer, header, &msg_buf, deadline));

  if (header->call_id() != kNegotiationCallId) {
    Status s = Status::IllegalState(Substitute(
          "Received invalid $0 $1; expected callId: $2, received callId: $3",
          tag, msg_type(), kNegotiationCallId, header->call_id()));
    LOG(DFATAL) << s.ToString();
    return s;
  }

  if (!msg->ParseFromArray(msg_buf.data(), msg_buf.size())) {
    return Status::IOError(Substitute("Received invalid $0 $1; missing fields", tag, msg_type()),
                           msg->InitializationErrorString());
  }

  TRACE("$0: Received $1 $2", direction(), tag, msg_type());
  return Status::OK();
}

// Sends a header and NegotiatePB to the connection. The header's call ID will
// be filled in.
template<typename Header>
static Status SendNegotiationMsg(Connection* conn,
                                 const MonoTime& deadline,
                                 Header* header,
                                 const NegotiatePB& msg) {
  header->set_call_id(kNegotiationCallId);
  TRACE("$0: Sending $1 $2",
        conn->direction() == Connection::SERVER ? "Server" : "Client",
        NegotiatePB::NegotiateStep_Name(msg.step()),
        conn->direction() == Connection::CLIENT ? "request" : "response");
  return SendFramedMessageBlocking(conn->socket(), *header, msg, deadline);
}

// Sends the initial NEGOTIATE message to the server.
static Status SendNegotiateRequest(Connection* conn, const MonoTime& deadline) {
  DCHECK_EQ(conn->direction(), Connection::CLIENT);
  NegotiatePB msg;
  msg.set_step(NegotiatePB::NEGOTIATE);

  // Advertise our supported features.
  for (RpcFeatureFlag feature : kSupportedClientRpcFeatureFlags) {
    msg.add_supported_features(feature);
  }

  // Add TLS if the connection is configured to support it.
  if (conn->IsTlsCapable()) {
    msg.add_supported_features(TLS);
  }

  RequestHeader header;
  return SendNegotiationMsg(conn, deadline, &header, msg);
}

// Receives the initial Negotiate message from the client.
// Fills in the connection's set of supported RPC feature flags.
static Status RecvNegotiateRequest(Connection* conn,
                                   faststring* buffer,
                                   const MonoTime& deadline) {
  DCHECK_EQ(conn->direction(), Connection::SERVER);
  RequestHeader header;
  NegotiatePB request;
  const string& tag = NegotiatePB::NegotiateStep_Name(NegotiatePB::NEGOTIATE);
  RETURN_NOT_OK(RecvNegotiationMsg(conn, tag, buffer, deadline, &header, &request));

  if (request.step() != NegotiatePB::NEGOTIATE) {
    return Status::NotAuthorized("Expected NEGOTIATE step; received",
                                 NegotiatePB::NegotiateStep_Name(request.step()));
  }

  // Fill in the set of features supported by the connection.
  for (int flag : request.supported_features()) {
    // Only set RPC features which both the local and remote ends support.
    RpcFeatureFlag feature_flag =
      RpcFeatureFlag_IsValid(flag) ? static_cast<RpcFeatureFlag>(flag) : UNKNOWN;
    if (ContainsKey(kSupportedServerRpcFeatureFlags, feature_flag) ||
        (feature_flag == TLS && conn->IsTlsCapable())) {
      conn->supported_features()->insert(feature_flag);
    }
  }

  return Status::OK();
}

static Status SendNegotiateResponse(Connection* conn, const MonoTime& deadline) {
  DCHECK_EQ(conn->direction(), Connection::SERVER);
  NegotiatePB msg;
  msg.set_step(NegotiatePB::NEGOTIATE);

  // Advertise our supported features.
  for (RpcFeatureFlag feature : kSupportedServerRpcFeatureFlags) {
    msg.add_supported_features(feature);
  }

  // Add TLS if the connection is configured to support it.
  if (conn->IsTlsCapable()) {
    msg.add_supported_features(TLS);
  }


  ResponseHeader header;
  return SendNegotiationMsg(conn, deadline, &header, msg);
}

static Status RecvNegotiateResponse(Connection* conn,
                                    faststring* buffer,
                                    const MonoTime& deadline) {
  DCHECK(conn->direction() == Connection::CLIENT);
  ResponseHeader header;
  NegotiatePB response;
  const string& tag = NegotiatePB::NegotiateStep_Name(NegotiatePB::NEGOTIATE);
  RETURN_NOT_OK(RecvNegotiationMsg(conn, tag, buffer, deadline, &header, &response));

  if (response.step() != NegotiatePB::NEGOTIATE) {
    return Status::NotAuthorized("Expected NEGOTIATE step; received",
                                 NegotiatePB::NegotiateStep_Name(response.step()));
  }

  // Fill in the set of features supported by the connection.
  for (int flag : response.supported_features()) {
    // Only set RPC features which both the local and remote ends support.
    RpcFeatureFlag feature_flag =
      RpcFeatureFlag_IsValid(flag) ? static_cast<RpcFeatureFlag>(flag) : UNKNOWN;
    if (ContainsKey(kSupportedClientRpcFeatureFlags, feature_flag) ||
        (feature_flag == TLS && conn->IsTlsCapable())) {
      conn->supported_features()->insert(feature_flag);
    }
  }

  return Status::OK();
}

static Status InitiateTlsHandshake(Connection* conn,
                                   const MonoTime& deadline,
                                   Connection::Direction direction) {
  return Status::OK();
}

static Status RecvConnectionHeader(Connection* conn, const MonoTime& deadline) {
  TRACE("Waiting for connection header");
  const uint8_t buflen = kMagicNumberLength + kHeaderFlagsLength;
  uint8_t buf[buflen];
  size_t nread;
  RETURN_NOT_OK(conn->socket()->BlockingRecv(buf, buflen, &nread, deadline));
  DCHECK_EQ(buflen, nread);

  RETURN_NOT_OK(serialization::ValidateConnHeader(Slice(buf, buflen)));
  TRACE("Connection header received");
  return Status::OK();
}

static Status SendConnectionHeader(Connection* conn, const MonoTime& deadline) {
  const uint8_t buflen = kMagicNumberLength + kHeaderFlagsLength;
  uint8_t buf[buflen];
  serialization::SerializeConnHeader(buf);
  size_t nsent;
  RETURN_NOT_OK(conn->socket()->BlockingWrite(buf, buflen, &nsent, deadline));
  DCHECK_EQ(buflen, nsent);
  return Status::OK();
}

// Wait for the client connection to be established and become ready for writing.
static Status WaitForClientConnect(Connection* conn, const MonoTime& deadline) {
  TRACE("Waiting for socket to connect");
  int fd = conn->socket()->GetFd();
  struct pollfd poll_fd;
  poll_fd.fd = fd;
  poll_fd.events = POLLOUT;
  poll_fd.revents = 0;

  MonoTime now;
  MonoDelta remaining;
  while (true) {
    now = MonoTime::Now();
    remaining = deadline - now;
    DVLOG(4) << "Client waiting to connect for negotiation, time remaining until timeout deadline: "
             << remaining.ToString();
    if (PREDICT_FALSE(remaining.ToNanoseconds() <= 0)) {
      return Status::TimedOut("Timeout exceeded waiting to connect");
    }
#if defined(__linux__)
    struct timespec ts;
    remaining.ToTimeSpec(&ts);
    int ready = ppoll(&poll_fd, 1, &ts, NULL);
#else
    int ready = poll(&poll_fd, 1, remaining.ToMilliseconds());
#endif
    if (ready == -1) {
      int err = errno;
      if (err == EINTR) {
        // We were interrupted by a signal, let's go again.
        continue;
      } else {
        return Status::NetworkError("Error from ppoll() while waiting to connect",
            ErrnoToString(err), err);
      }
    } else if (ready == 0) {
      // Timeout exceeded. Loop back to the top to our impending doom.
      continue;
    } else {
      // Success.
      break;
    }
  }

  // Connect finished, but this doesn't mean that we connected successfully.
  // Check the socket for an error.
  int so_error = 0;
  socklen_t socklen = sizeof(so_error);
  int rc = getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_error, &socklen);
  if (rc != 0) {
    return Status::NetworkError("Unable to check connected socket for errors",
                                ErrnoToString(errno),
                                errno);
  }
  if (so_error != 0) {
    return Status::NetworkError("connect", ErrnoToString(so_error), so_error);
  }

  return Status::OK();
}

// Disable / reset socket timeouts.
static Status DisableSocketTimeouts(Connection* conn) {
  RETURN_NOT_OK(conn->socket()->SetSendTimeout(MonoDelta::FromNanoseconds(0L)));
  RETURN_NOT_OK(conn->socket()->SetRecvTimeout(MonoDelta::FromNanoseconds(0L)));
  return Status::OK();
}

// Perform client negotiation. We don't LOG() anything, we leave that to our caller.
static Status DoClientNegotiation(Connection* conn,
                                  const MonoTime& deadline) {
  // The SASL initialization on the client side can be relatively heavy-weight
  // (it may result in DNS queries in the case of GSSAPI).
  // So, we do it while the connect() is still in progress to reduce latency.
  //
  // TODO(todd): we should consider doing this even before connecting, since as soon
  // as we connect, we are tying up a negotiation thread on the server side.

  RETURN_NOT_OK(conn->InitSaslClient());

  RETURN_NOT_OK(WaitForClientConnect(conn, deadline));
  RETURN_NOT_OK(conn->SetNonBlocking(false));
  RETURN_NOT_OK(conn->InitSSLIfNecessary());
  conn->sasl_client().set_deadline(deadline);
  RETURN_NOT_OK(SendConnectionHeader(conn, deadline));
  RETURN_NOT_OK(conn->sasl_client().Negotiate());
  RETURN_NOT_OK(SendConnectionContext(conn, deadline));
  RETURN_NOT_OK(DisableSocketTimeouts(conn));

  return Status::OK();
}

// Perform server negotiation. We don't LOG() anything, we leave that to our caller.
static Status DoServerNegotiation(Connection* conn,
                                  const MonoTime& deadline) {
  if (FLAGS_rpc_negotiation_inject_delay_ms > 0) {
    LOG(WARNING) << "Injecting " << FLAGS_rpc_negotiation_inject_delay_ms
                 << "ms delay in negotiation";
    SleepFor(MonoDelta::FromMilliseconds(FLAGS_rpc_negotiation_inject_delay_ms));
  }
  RETURN_NOT_OK(conn->SetNonBlocking(false));
  RETURN_NOT_OK(conn->InitSSLIfNecessary());
  RETURN_NOT_OK(conn->InitSaslServer());
  conn->sasl_server().set_deadline(deadline);
  RETURN_NOT_OK(RecvConnectionHeader(conn, deadline));
  RETURN_NOT_OK(conn->sasl_server().Negotiate());
  RETURN_NOT_OK(RecvConnectionContext(conn, deadline));
  RETURN_NOT_OK(DisableSocketTimeouts(conn));

  return Status::OK();
}

// Perform negotiation for a connection (either server or client)
void Negotiation::RunNegotiation(const scoped_refptr<Connection>& conn,
                                 const MonoTime& deadline) {
  Status s;
  if (conn->direction() == Connection::SERVER) {
    s = DoServerNegotiation(conn.get(), deadline);
  } else {
    s = DoClientNegotiation(conn.get(), deadline);
  }

  if (PREDICT_FALSE(!s.ok())) {
    string msg = Substitute("$0 connection negotiation failed: $1",
                            conn->direction() == Connection::SERVER ? "Server" : "Client",
                            conn->ToString());
    s = s.CloneAndPrepend(msg);
  }
  TRACE("Negotiation complete: $0", s.ToString());

  bool is_bad = !s.ok() && !(
      (s.IsNetworkError() && s.posix_code() == ECONNREFUSED) ||
      s.IsNotAuthorized());

  if (is_bad || FLAGS_rpc_trace_negotiation) {
    string msg = Trace::CurrentTrace()->DumpToString();
    if (is_bad) {
      LOG(WARNING) << "Failed RPC negotiation. Trace:\n" << msg;
    } else {
      LOG(INFO) << "RPC negotiation tracing enabled. Trace:\n" << msg;
    }
  }

  if (conn->direction() == Connection::SERVER && s.IsNotAuthorized()) {
    LOG(WARNING) << "Unauthorized connection attempt: " << s.message().ToString();
  }
  conn->CompleteNegotiation(s);
}


} // namespace rpc
} // namespace kudu
