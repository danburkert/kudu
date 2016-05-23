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

#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "kudu/common/partial_row.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/integration-tests/mini_cluster.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/mini_master.h"
#include "kudu/rpc/messenger.h"
#include "kudu/util/test_util.h"

using std::make_tuple;
using std::shared_ptr;
using std::string;
using std::tuple;
using std::unique_ptr;
using std::vector;

namespace kudu {
using rpc::Messenger;
using rpc::MessengerBuilder;
using rpc::RpcController;
namespace master {

const int kNumTabletServers = 3;

// Test the master GetTableLocations RPC. This can't be done in master-test,
// since it requires a running tablet server in order for the catalog manager to
// report tablet locations.
class TableLocationsTest : public KuduTest {
 public:

  void SetUp() override {
    KuduTest::SetUp();

    MiniClusterOptions opts;
    opts.num_tablet_servers = kNumTabletServers;

    cluster_.reset(new MiniCluster(env_.get(), opts));
    ASSERT_OK(cluster_->Start());
    ASSERT_OK(cluster_->WaitForTabletServerCount(kNumTabletServers));

    // Create a client proxy to the master.
    MessengerBuilder bld("Client");
    ASSERT_OK(bld.Build(&client_messenger_));
    proxy_.reset(new MasterServiceProxy(client_messenger_,
                                        cluster_->leader_mini_master()->bound_rpc_addr()));
  }

  void TearDown() override {
    if (cluster_) {
      cluster_->Shutdown();
      cluster_.reset();
    }
    KuduTest::TearDown();
  }

  Status CreateTable(const string& table_name,
                     const Schema& schema,
                     const vector<KuduPartialRow>& split_rows,
                     const vector<tuple<KuduPartialRow, KuduPartialRow>>& bounds);

  shared_ptr<Messenger> client_messenger_;
  unique_ptr<MiniCluster> cluster_;
  unique_ptr<MasterServiceProxy> proxy_;
};

Status TableLocationsTest::CreateTable(const string& table_name,
                                       const Schema& schema,
                                       const vector<KuduPartialRow>& split_rows,
                                       const vector<tuple<KuduPartialRow,
                                                          KuduPartialRow>>& bounds) {

  CreateTableRequestPB req;
  CreateTableResponsePB resp;
  RpcController controller;

  req.set_name(table_name);
  RETURN_NOT_OK(SchemaToPB(schema, req.mutable_schema()));
  RowOperationsPBEncoder encoder(req.mutable_split_rows_range_bounds());
  for (const KuduPartialRow& row : split_rows) {
    encoder.Add(RowOperationsPB::SPLIT_ROW, row);
  }
  for (const tuple<KuduPartialRow, KuduPartialRow>& bound : bounds) {
    encoder.Add(RowOperationsPB::RANGE_LOWER_BOUND, std::get<0>(bound));
    encoder.Add(RowOperationsPB::RANGE_UPPER_BOUND, std::get<1>(bound));
  }

  RETURN_NOT_OK(proxy_->CreateTable(req, &resp, &controller));
  if (resp.has_error()) {
    RETURN_NOT_OK(StatusFromPB(resp.error().status()));
  }

  IsCreateTableDoneRequestPB done_req;
  IsCreateTableDoneResponsePB done_resp;
  done_req.mutable_table()->set_table_name(table_name);

  for (int i = 1; i <= 20; i++) {
    controller.Reset();
    RETURN_NOT_OK(proxy_->IsCreateTableDone(done_req, &done_resp, &controller));
    if (done_resp.has_error()) {
      RETURN_NOT_OK(StatusFromPB(done_resp.error().status()));
    }
    if (done_resp.done()) {
      return Status::OK();
    } else {
      SleepFor(MonoDelta::FromMilliseconds(i * 100));
    }
  }
  return Status::TimedOut("CreateTable", req.DebugString());
}

// Test that when the client requests table locations for a non-covered
// partition range, the master returns the first tablet previous to the begin
// partition key, as specified in the non-covering range partitions design
// document.
TEST_F(TableLocationsTest, TestGetTableLocations) {
  const string table_name = "test";
  Schema schema({ ColumnSchema("key", STRING) }, 1);
  KuduPartialRow row = KuduPartialRow(&schema);

  vector<KuduPartialRow> splits(6, row);
  ASSERT_OK(splits[0].SetString(0, "aa"));
  ASSERT_OK(splits[1].SetString(0, "ab"));
  ASSERT_OK(splits[2].SetString(0, "ac"));
  ASSERT_OK(splits[3].SetString(0, "ca"));
  ASSERT_OK(splits[4].SetString(0, "cb"));
  ASSERT_OK(splits[5].SetString(0, "cc"));

  vector<tuple<KuduPartialRow, KuduPartialRow>> bounds(2, make_tuple(row, row));
  ASSERT_OK(std::get<0>(bounds[0]).SetString(0, "a"));
  ASSERT_OK(std::get<1>(bounds[0]).SetString(0, "b"));
  ASSERT_OK(std::get<0>(bounds[1]).SetString(0, "c"));
  ASSERT_OK(std::get<1>(bounds[1]).SetString(0, "d"));

  ASSERT_OK(CreateTable(table_name, schema, splits, bounds));

  { // from "a"
    GetTableLocationsRequestPB req;
    GetTableLocationsResponsePB resp;
    RpcController controller;
    req.mutable_table()->set_table_name(table_name);
    req.set_partition_key_start("a");
    req.set_max_returned_locations(3);
    ASSERT_OK(proxy_->GetTableLocations(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());

    ASSERT_TRUE(!resp.has_error());
    ASSERT_EQ(3, resp.tablet_locations().size());
    EXPECT_EQ("a", resp.tablet_locations(0).partition().partition_key_start());
    EXPECT_EQ("aa", resp.tablet_locations(1).partition().partition_key_start());
    EXPECT_EQ("ab", resp.tablet_locations(2).partition().partition_key_start());
  }

  { // from ""
    GetTableLocationsRequestPB req;
    GetTableLocationsResponsePB resp;
    RpcController controller;
    req.mutable_table()->set_table_name(table_name);
    req.set_partition_key_start("");
    req.set_max_returned_locations(3);
    ASSERT_OK(proxy_->GetTableLocations(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());

    ASSERT_TRUE(!resp.has_error());
    ASSERT_EQ(3, resp.tablet_locations().size());
    EXPECT_EQ("a", resp.tablet_locations(0).partition().partition_key_start());
    EXPECT_EQ("aa", resp.tablet_locations(1).partition().partition_key_start());
    EXPECT_EQ("ab", resp.tablet_locations(2).partition().partition_key_start());
  }

  { // from "b"
    GetTableLocationsRequestPB req;
    GetTableLocationsResponsePB resp;
    RpcController controller;
    req.mutable_table()->set_table_name(table_name);
    req.set_partition_key_start("b");
    req.set_max_returned_locations(3);
    ASSERT_OK(proxy_->GetTableLocations(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());

    ASSERT_TRUE(!resp.has_error());
    ASSERT_EQ(3, resp.tablet_locations().size());
    EXPECT_EQ("ac", resp.tablet_locations(0).partition().partition_key_start());
    EXPECT_EQ("c", resp.tablet_locations(1).partition().partition_key_start());
    EXPECT_EQ("ca", resp.tablet_locations(2).partition().partition_key_start());
  }

  { // from "z"
    GetTableLocationsRequestPB req;
    GetTableLocationsResponsePB resp;
    RpcController controller;
    req.mutable_table()->set_table_name(table_name);
    req.set_partition_key_start("z");
    req.set_max_returned_locations(3);
    ASSERT_OK(proxy_->GetTableLocations(req, &resp, &controller));
    SCOPED_TRACE(resp.DebugString());

    ASSERT_TRUE(!resp.has_error());
    ASSERT_EQ(1, resp.tablet_locations().size());
    EXPECT_EQ("cc", resp.tablet_locations(0).partition().partition_key_start());
  }
}


} // namespace master
} // namespace kudu
