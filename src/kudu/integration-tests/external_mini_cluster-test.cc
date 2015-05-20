// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

#include "kudu/integration-tests/external_mini_cluster.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/test_util.h"

namespace kudu {

class EMCTest : public KuduTest {
 public:
  EMCTest() {
    // Hard-coded RPC ports for the masters. This is safe, as this unit test
    // runs under a resource lock (see CMakeLists.txt in this directory).
    // TODO we should have a generic method to obtain n free ports.
    master_quorum_ports_ = { 11010, 11011, 11012 };
  }

 protected:
  std::vector<uint16_t> master_quorum_ports_;
};

TEST_F(EMCTest, TestBasicOperation) {
  ExternalMiniClusterOptions opts;
  opts.num_masters = master_quorum_ports_.size();
  opts.num_tablet_servers = 3;
  opts.master_rpc_ports = master_quorum_ports_;

  ExternalMiniCluster cluster(opts);
  ASSERT_OK(cluster.Start());

  // Verify that the masters have bound their RPC and HTTP ports.
  for (int i = 0; i < opts.num_masters; i++) {
    SCOPED_TRACE(i);
    ExternalMaster* master = CHECK_NOTNULL(cluster.master(i));
    HostPort master_rpc = master->bound_rpc_hostport();
    EXPECT_TRUE(HasPrefixString(master_rpc.ToString(), "127.0.0.1:")) << master_rpc.ToString();

    HostPort master_http = master->bound_http_hostport();
    EXPECT_TRUE(HasPrefixString(master_http.ToString(), "127.0.0.1:")) << master_http.ToString();
  }

  // Verify each of the tablet servers.
  for (int i = 0; i < opts.num_tablet_servers; i++) {
    SCOPED_TRACE(i);
    ExternalTabletServer* ts = CHECK_NOTNULL(cluster.tablet_server(i));
    HostPort ts_rpc = ts->bound_rpc_hostport();
    string expected_prefix = strings::Substitute("$0:", cluster.GetBindIpForTabletServer(i));
    EXPECT_NE(expected_prefix, "127.0.0.1") << "Should bind to unique per-server hosts";
    EXPECT_TRUE(HasPrefixString(ts_rpc.ToString(), expected_prefix)) << ts_rpc.ToString();

    HostPort ts_http = ts->bound_http_hostport();
    EXPECT_TRUE(HasPrefixString(ts_http.ToString(), "127.0.0.1:")) << ts_http.ToString();
  }

  // Restart a master and a tablet server. Make sure they come back up with the same ports.
  ExternalMaster* master = cluster.master(0);
  HostPort master_rpc = master->bound_rpc_hostport();
  HostPort master_http = master->bound_http_hostport();

  master->Shutdown();
  ASSERT_OK(master->Restart());

  ASSERT_EQ(master_rpc.ToString(), master->bound_rpc_hostport().ToString());
  ASSERT_EQ(master_http.ToString(), master->bound_http_hostport().ToString());

  ExternalTabletServer* ts = cluster.tablet_server(0);

  HostPort ts_rpc = ts->bound_rpc_hostport();
  HostPort ts_http = ts->bound_http_hostport();

  ts->Shutdown();
  ASSERT_OK(ts->Restart());

  ASSERT_EQ(ts_rpc.ToString(), ts->bound_rpc_hostport().ToString());
  ASSERT_EQ(ts_http.ToString(), ts->bound_http_hostport().ToString());

  cluster.Shutdown();
}

} // namespace kudu
