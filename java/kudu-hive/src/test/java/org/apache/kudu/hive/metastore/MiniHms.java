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

package org.apache.kudu.hive.metastore;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;

import com.sun.security.auth.module.UnixSystem;
import org.apache.hadoop.hive.conf.HiveConf;

import org.apache.kudu.client.TestUtils;

public class MiniHms implements AutoCloseable {

  private int port;
  private HiveConf conf;
  private File tempFile;


  public static MiniHms create() throws Exception {
    String baseDir = TestUtils.getBaseDir();
    new File(TestUtils.getBaseDir() +
             File.pathSeparator + "hms" +
             File.pathSeparator + "warehouse").mkdirs();



    /*
    HiveConf conf = new HiveConf();

    conf.set(HiveConf.ConfVars.METASTOREURIS.varname, "thrift://localhost:" + port);
    conf.set(HiveConf.ConfVars.METASTORE_TRANSACTIONAL_EVENT_LISTENERS.varname,
             KuduMetastorePlugin.class.getCanonicalName());

    File.createTempFile("kudu-hive");
    */


    return new MiniHms();
  }

  public int getPort() {
    return port;
  }

  public HiveConf getConf() {
    return conf;
  }

  @Override
  public void close() throws Exception {
  }

  private static HiveConf createConf() throws Exception {
    HiveConf hiveConf = new HiveConf();

    /*
    File tempFile = File.createTempFile("kudu-hive");

    hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, "thrift://localhost:" + port);
    hiveConf.set(HiveConf.ConfVars.METASTORE_TRANSACTIONAL_EVENT_LISTENERS.varname,
                 KuduMetastorePlugin.class.getCanonicalName());

    File.createTempFile("kudu-hive");
    */

    /*
    hiveConf.set(HiveConf.ConfVars.D);
    <name>fs.default.name</name>
    <value>file://$1/fs/</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>file://$1/fs/warehouse/</value>
  </property>
  */

    return hiveConf;
  }
}
