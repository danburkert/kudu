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

#include <csignal>
#include <cstdlib>
#include <map>
#include <memory>
#include <string>

#include <glog/logging.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/monotime.h"
#include "kudu/util/path_util.h"
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

  Subprocess schematool({
        Substitute("$0/bin/schematool", hive_home),
        "-dbType", "derby", "-initSchema",
  });
  schematool.SetEnvVars(env_vars);
  schematool.SetCurrentDir(tmp_dir);
  RETURN_NOT_OK(schematool.Start());
  RETURN_NOT_OK(schematool.Wait());
  int rc;
  RETURN_NOT_OK(schematool.GetExitStatus(&rc));
  if (rc != 0) {
    return Status::RuntimeError("schematool failed");
  }

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
  return WaitForTcpBind(hms_process_->pid(), &port_, MonoDelta::FromSeconds(10));
}

Status MiniHms::CreateHiveSite(const string& tmp_dir) const {
  static const string kFileTemplate = R"(
    <properties>
      <property>
        <name>hive.metastore.transactional.event.listeners</name>
        <value>
          org.apache.hive.hcatalog.listener.DbNotificationListener,
          org.apache.kudu.hive.metastore.KuduMetastorePlugin
        </value>
      </property>
      <property>
        <name>hive.metastore.event.db.listener.timetolive</name>
        <value>$0s</value>
      </property>
      <property>
        <name>javax.jdo.option.ConnectionURL</name>
        <value>jdbc:derby:;databaseName=$1/metastore_db;create=true</value>
      </property>
      <property>
        <name>fs.default.name</name>
        <value>file://$1/fs/</value>
      </property>
      <property>
        <name>hive.metastore.warehouse.dir</name>
        <value>file://$1/fs/warehouse/</value>
      </property>
    </properties>
  )";

  string file_contents = strings::Substitute(kFileTemplate,
                                             notification_log_ttl_.ToSeconds(),
                                             tmp_dir);

  RETURN_NOT_OK(Env::Default()->CreateDir(JoinPathSegments(tmp_dir, "fs")));
  RETURN_NOT_OK(Env::Default()->CreateDir(JoinPathSegments(tmp_dir, "fs/warehouse")));

  return WriteStringToFile(Env::Default(),
                           file_contents,
                           JoinPathSegments(tmp_dir, "hive-site.xml"));
}

} // namespace kudu
