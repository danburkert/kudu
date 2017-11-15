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

#include "kudu/hms/sasl_client_transport.h"

#include <algorithm>
#include <memory>
#include <string>

#include <glog/logging.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TVirtualTransport.h>

#include "kudu/gutil/endian.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/sasl_common.h"
#include "kudu/rpc/sasl_helper.h"
#include "kudu/util/faststring.h"
#include "kudu/util/logging.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

using apache::thrift::transport::TTransportException;
using std::shared_ptr;
using std::string;
using strings::Substitute;

namespace kudu {

using rpc::SaslMechanism;
using rpc::WrapSaslCall;

namespace hms {

namespace {

// SASL negotiation frames are sent with an 8-bit status and a 32-bit length.
const uint32_t kSaslHeaderWidth = sizeof(uint8_t) + sizeof(uint32_t);

// Frame headers consist of a 32-bit length.
const uint32_t kFrameHeaderWidth = sizeof(uint32_t);

// SASL SASL_CB_GETOPT callback function.
int GetoptCb(SaslClientTransport* client_transport,
             const char* plugin_name,
             const char* option,
             const char** result,
             unsigned* len) {
  return client_transport->GetOptionCb(plugin_name, option, result, len);
}

// SASL SASL_CB_CANON_USER callback function.
static int CanonUserCb(sasl_conn_t* conn,
                       void* context,
                       const char* in, unsigned inlen,
                       unsigned flags,
                       const char *user_realm,
                       char* out,
                       unsigned out_max, unsigned* out_len) {
  memcpy(out, in, inlen);
  *out_len = inlen;
  return SASL_OK;
}

// SASL SASL_CB_USER callback function.
int UserCb(void* context, int id, const char** result, unsigned* len) {
  CHECK_EQ(SASL_CB_USER, id);

  // Setting the username to the empty string causes the remote end to use the
  // clients Kerberos principal, which is correct.
  *result = "";
  if (len != nullptr) *len = 0;
  return SASL_OK;
}
} // anonymous namespace

SaslClientTransport::SaslClientTransport(const string& server_fqdn,
                                         shared_ptr<TTransport> transport)
    : transport_(std::move(transport)),
      sasl_helper_(rpc::SaslHelper::CLIENT),
      sasl_callbacks_({
          rpc::SaslBuildCallback(SASL_CB_GETOPT, reinterpret_cast<int (*)()>(&GetoptCb), this),
          rpc::SaslBuildCallback(SASL_CB_CANON_USER,
                                 reinterpret_cast<int (*)()>(&CanonUserCb),
                                 this),
          rpc::SaslBuildCallback(SASL_CB_USER, reinterpret_cast<int (*)()>(&UserCb), nullptr),
          rpc::SaslBuildCallback(SASL_CB_LIST_END, nullptr, nullptr)
      }),
      max_frame_len_(rpc::kSaslMaxOutBufLen),
      needs_wrap_(false) {
  sasl_helper_.set_server_fqdn(server_fqdn);
  sasl_helper_.EnableGSSAPI();
  ResetWriteBuf();
}

bool SaslClientTransport::isOpen() {
  return transport_->isOpen();
}

bool SaslClientTransport::peek() {
  return !read_slice_.empty() || transport_->peek();
}

void SaslClientTransport::open() {
  transport_->open();
  try {
    Negotiate();
  } catch (...) {
    transport_->close();
    throw;
  }
}

void SaslClientTransport::close() {
  transport_->close();
  sasl_conn_.reset();
}

void SaslClientTransport::ReadFrame() {
  DCHECK_EQ(0, read_buf_.size());
  DCHECK(read_slice_.empty());

  read_buf_.resize(kFrameHeaderWidth);
  transport_->readAll(read_buf_.data(), kFrameHeaderWidth);
  size_t payload_len = NetworkByteOrder::Load32(read_buf_.data());

  if (payload_len > max_frame_len_) {
    throw TTransportException(Substitute("Thrift SASL frame is too long: $0/$1",
                                         payload_len, max_frame_len_));
  }

  read_buf_.resize(kFrameHeaderWidth + payload_len);
  transport_->readAll(&read_buf_.data()[kFrameHeaderWidth], payload_len);

  if (needs_wrap_) {
    // Point read_slice_ directly at the SASL library's internal buffer. This
    // avoids having to copy the decoded data back into read_buf_.
    Status s = rpc::SaslDecode(sasl_conn_.get(), read_buf_, &read_slice_);
    if (!s.ok()) {
      throw SaslException(s.CloneAndPrepend("failed to SASL decode data"));
    }
    ResetReadBuf();
  } else {
    read_slice_ = read_buf_;
    read_slice_.remove_prefix(kFrameHeaderWidth);
  }
}

uint32_t SaslClientTransport::read(uint8_t* buf, uint32_t len) {
  // Copy data from the read slice into the output buffer,
  // returning the amount of data copied.
  auto read_fast = [this, buf, len] {
    uint32_t n = std::min(read_slice_.size(), static_cast<size_t>(len));
    memcpy(buf, read_slice_.data(), n);
    read_slice_.remove_prefix(n);
    if (read_slice_.empty()) {
      ResetReadBuf();
    }
    return n;
  };

  // If we have some data in the read buffer, then return it.
  if (!read_slice_.empty()) {
    return read_fast();
  }

  // Otherwise, read a new frame from the wire and return it.
  ReadFrame();
  return read_fast();
}

void SaslClientTransport::write(const uint8_t* buf, uint32_t len) {
  // Check that we've already preallocated space in the buffer for the frame-header.
  DCHECK(write_buf_.size() >= kFrameHeaderWidth);

  // Check if the amount to write would overflow a frame.
  while (write_buf_.size() + len > max_frame_len_) {
    uint32_t n = max_frame_len_ - write_buf_.size();
    write_buf_.append(buf, n);
    flush();
    buf += n;
    len -= n;
  }

  write_buf_.append(buf, len);
}

void SaslClientTransport::flush() {
  if (needs_wrap_) {
    Slice plaintext(write_buf_);
    plaintext.remove_prefix(kFrameHeaderWidth);
    Slice ciphertext;
    Status s = rpc::SaslEncode(sasl_conn_.get(), plaintext, &ciphertext);
    if (!s.ok()) {
      throw SaslException(s.CloneAndPrepend("failed to SASL encode data"));
    }

    // Note: when the SASL C library encodes the plaintext, it prefixes the
    // ciphertext with the length. Since this happens to match the SASL/Thrift
    // frame format, we can send the ciphertext unmodified to the remote server.
    transport_->write(ciphertext.data(), ciphertext.size());
  } else {
    size_t payload_len = write_buf_.size() - kFrameHeaderWidth;
    NetworkByteOrder::Store32(write_buf_.data(), payload_len);
    transport_->write(write_buf_.data(), write_buf_.size());
  }

  transport_->flush();
  ResetWriteBuf();
}

void SaslClientTransport::Negotiate() {
  SetupSaslContext();

  faststring recv_buf;
  SendSaslStart();

  for (;;) {
    recv_buf.clear();
    NegotiationStatus status = ReceiveSaslMessage(&recv_buf);

    if (status == TSASL_COMPLETE) {
        throw SaslException(
            Status::IllegalState("Received SASL COMPLETE status, but handshake is not finished"));
    }
    CHECK_EQ(status, TSASL_OK);

    const char* out;
    unsigned out_len;
    Status s = WrapSaslCall(sasl_conn_.get(), [&] {
        return sasl_client_step(sasl_conn_.get(),
                                reinterpret_cast<const char*>(recv_buf.data()),
                                recv_buf.size(),
                                nullptr,
                                &out,
                                &out_len);
    });

    if (PREDICT_FALSE(!s.IsIncomplete() && !s.ok())) {
      throw SaslException(std::move(s));
    }

    SendSaslMessage(status, Slice(out, out_len));
    transport_->flush();

    if (s.ok()) {
      break;
    }
  }

  recv_buf.clear();
  NegotiationStatus status = ReceiveSaslMessage(&recv_buf);
  if (status != TSASL_COMPLETE) {
    throw SaslException(
        Status::IllegalState("Received SASL OK status, but expected SASL COMPLETE"));
  }
  DCHECK_EQ(0, recv_buf.size());

  needs_wrap_ = rpc::NeedsWrap(sasl_conn_.get());
  max_frame_len_ = rpc::GetMaxBufferSize(sasl_conn_.get());

  VLOG(2) << "SASL GSSAPI negotiation complete; needs wrap: " << (needs_wrap_ ? "true" : "false")
          << "; max frame length: " << max_frame_len_;
}

void SaslClientTransport::SendSaslMessage(NegotiationStatus status, Slice payload) {
  uint8_t header[kSaslHeaderWidth];
  header[0] = status;
  NetworkByteOrder::Store32(&header[1], payload.size());
  transport_->write(header, kSaslHeaderWidth);
  if (!payload.empty()) {
    transport_->write(payload.data(), payload.size());
  }
}

NegotiationStatus SaslClientTransport::ReceiveSaslMessage(faststring* payload) {
  // Read the fixed-length message header.
  uint8_t header[kSaslHeaderWidth];
  transport_->readAll(header, kSaslHeaderWidth);
  size_t len = NetworkByteOrder::Load32(&header[1]);

  // Handle status errors.
  switch (header[0]) {
    case TSASL_OK:
    case TSASL_COMPLETE: break;
    case TSASL_BAD:
    case TSASL_ERROR:
      throw SaslException(Status::RuntimeError("SASL peer indicated failure"));
    // The Thrift client should never receive TSASL_START.
    case TSASL_START:
    default:
      throw SaslException(Status::RuntimeError("Unexpected SASL status",
                                               std::to_string(header[0])));
  }

  // Read the message payload.
  if (len > max_frame_len_) {
    throw SaslException(Status::RuntimeError(
          "SASL negotiation message payload exceeds maximum length"));
  }
  payload->resize(len);
  transport_->readAll(payload->data(), len);

  return static_cast<NegotiationStatus>(header[0]);
}

void SaslClientTransport::SendSaslStart() {
  const char* init_msg = nullptr;
  unsigned init_msg_len = 0;
  const char* negotiated_mech = nullptr;

  Status s = WrapSaslCall(sasl_conn_.get(), [&] {
      return sasl_client_start(
          sasl_conn_.get(),            // The SASL connection context created by sasl_client_new()
          SaslMechanism::name_of(SaslMechanism::GSSAPI), // The mechanism to use.
          nullptr,                                       // Disables INTERACT return if NULL.
          &init_msg,                                     // Filled in on success.
          &init_msg_len,                                 // Filled in on success.
          &negotiated_mech);                             // Filled in on success.
  });

  if (PREDICT_FALSE(!s.IsIncomplete() && !s.ok())) {
    throw SaslException(std::move(s));
  }

  // Check that the SASL library is using the mechanism that we picked.
  DCHECK_EQ(SaslMechanism::value_of(negotiated_mech), SaslMechanism::GSSAPI);
  s = rpc::AllowProtection(sasl_conn_.get());
  if (!s.ok()) {
    throw SaslException(s);
  }

  // These two calls comprise a single message in the thrift-sasl protocol.
  SendSaslMessage(TSASL_START, Slice(negotiated_mech));
  SendSaslMessage(TSASL_OK, Slice(init_msg, init_msg_len));
  transport_->flush();
}

int SaslClientTransport::GetOptionCb(const char* plugin_name, const char* option,
                                     const char** result, unsigned* len) {
  return sasl_helper_.GetOptionCb(plugin_name, option, result, len);
}

void SaslClientTransport::SetupSaslContext() {
  sasl_conn_t* sasl_conn = nullptr;
  Status s = WrapSaslCall(nullptr /* no conn */, [&] {
      return sasl_client_new(
          "hive",                       // Registered name of the service using SASL. Required.
          sasl_helper_.server_fqdn(),   // The fully qualified domain name of the remote server.
          nullptr,                      // Local and remote IP address strings. (we don't use
          nullptr,                      // any mechanisms which require this info.)
          sasl_callbacks_.data(),       // Connection-specific callbacks.
          0,                            // flags
          &sasl_conn);
      });
  if (!s.ok()) {
    throw SaslException(s);
  }
  sasl_conn_.reset(sasl_conn);
}

void SaslClientTransport::ResetReadBuf() {
  read_buf_.resize(0);
  read_buf_.shrink_to_fit();
}

void SaslClientTransport::ResetWriteBuf() {
  write_buf_.resize(kFrameHeaderWidth);
  write_buf_.shrink_to_fit();
}

} // namespace hms
} // namespace kudu
