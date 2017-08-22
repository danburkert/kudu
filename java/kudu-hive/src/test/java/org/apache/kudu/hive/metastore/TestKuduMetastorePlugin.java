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

import org.junit.Test;

import org.apache.kudu.client.BaseKuduTest;
import org.apache.kudu.client.TestKuduClient;

public class TestKuduMetastorePlugin extends BaseKuduTest {

  @Test
  public void testCreateTable() throws Throwable {

    MiniHms mini_hms = MiniHms.create();

    Thread.sleep(10000);

    mini_hms.close();




//    HiveMetaStore.startMetaStore(port, ShimLoader.getHadoopThriftAuthBridge());





//    HiveMetaStoreClient hmsClient = new HiveMetaStoreClient(new HiveConf());

//    hmsClient.createDatabase(new Database("my_db",
//                                          null,
//                                          null,
//                                          Collections.<String, String>emptyMap()));

//    hmsClient.close();
  }
}
