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

#include "kudu/hms/mini_hms.h"

#include <algorithm>
#include <csignal>
#include <cstdlib>
#include <map>
#include <memory>
#include <ostream>
#include <string>

#include <glog/logging.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/monotime.h"
#include "kudu/util/path_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/string_case.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_util.h"

using std::map;
using std::string;
using std::unique_ptr;
using strings::Substitute;

namespace kudu {
namespace hms {

MiniHms::~MiniHms() {
  if (hms_process_) {
    VLOG(1) << "Stopping HMS";
    unique_ptr<Subprocess> proc = std::move(hms_process_);
    WARN_NOT_OK(proc->KillAndWait(SIGTERM), "failed to stop the Hive MetaStore process");
  }
}

namespace {

Status FindHomeDir(const char* name, const string& bin_dir, string* home_dir) {
  string name_upper;
  ToUpperCase(name, &name_upper);

  string env_var = Substitute("$0_HOME", name_upper);
  const char* env = std::getenv(env_var.c_str());
  *home_dir = env == nullptr ? JoinPathSegments(bin_dir, Substitute("$0-home", name)) : env;

  if (!Env::Default()->FileExists(*home_dir)) {
    return Status::NotFound(Substitute("$0 directory does not exist", env_var), *home_dir);
  }
  return Status::OK();
}

} // anonymous namespace

Status MiniHms::Start() {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, 20000, "Starting HMS");
  CHECK(!hms_process_);

  VLOG(1) << "Starting HMS";

  Env* env = Env::Default();

  string exe;
  RETURN_NOT_OK(env->GetExecutablePath(&exe));
  const string bin_dir = DirName(exe);

  string hadoop_home;
  string hive_home;
  string java_home;
  RETURN_NOT_OK(FindHomeDir("hadoop", bin_dir, &hadoop_home));
  RETURN_NOT_OK(FindHomeDir("hive", bin_dir, &hive_home));
  RETURN_NOT_OK(FindHomeDir("java", bin_dir, &java_home));

  auto tmp_dir = GetTestDataDirectory();

  RETURN_NOT_OK(CreateHiveSite(tmp_dir));

  // Comma-separated list of additional jars to add to the HMS classpath.
  string aux_jars = Substitute("$0/hcatalog/share/hcatalog,$1/hms-plugin.jar",
                               hive_home, bin_dir);
  map<string, string> env_vars {
      { "JAVA_HOME", java_home },
      { "HADOOP_HOME", hadoop_home },
      { "HIVE_AUX_JARS_PATH", aux_jars },
      { "HIVE_CONF_DIR", tmp_dir },
  };

  // Start the HMS.
  hms_process_.reset(new Subprocess({
        Substitute("$0/bin/hive", hive_home),
        "--service", "metastore",
        "-v",
        "-p", "0", // Use an ephemeral port.
  }));

  hms_process_->SetEnvVars(env_vars);
  RETURN_NOT_OK(hms_process_->Start());

  // Wait for HMS to start listening on its ports and commencing operation.
  VLOG(1) << "Waiting for HMS ports";
  return WaitForTcpBind(hms_process_->pid(), &port_, MonoDelta::FromSeconds(20));
}

Status MiniHms::CreateHiveSite(const string& tmp_dir) const {
  // 'datanucleus.schema.autoCreateAll' and 'hive.metastore.schema.verification'
  // allow Hive to startup and run without first running the schemaTool.
  // 'hive.metastore.event.db.listener.timetolive' configures how long the
  // Metastore will store notification log events before GCing them.
  static const string kFileTemplate = R"(
<configuration>
  <property>
    <name>hive.metastore.transactional.event.listeners</name>
    <value>
      org.apache.hive.hcatalog.listener.DbNotificationListener,
      org.apache.kudu.hive.metastore.KuduMetastorePlugin
    </value>
  </property>

  <property>
    <name>datanucleus.schema.autoCreateAll</name>
    <value>true</value>
  </property>

  <property>
    <name>hive.metastore.schema.verification</name>
    <value>false</value>
  </property>

  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>file://$1/warehouse/</value>
  </property>

  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:derby:memory:$1/metadb;create=true</value>
  </property>

  <property>
    <name>hive.metastore.event.db.listener.timetolive</name>
    <value>$0s</value>
  </property>

</configuration>
  )";

  string file_contents = strings::Substitute(kFileTemplate,
                                             notification_log_ttl_.ToSeconds(),
                                             tmp_dir);

  return WriteStringToFile(Env::Default(),
                           file_contents,
                           JoinPathSegments(tmp_dir, "hive-site.xml"));
}

} // namespace hms
} // namespace kudu
