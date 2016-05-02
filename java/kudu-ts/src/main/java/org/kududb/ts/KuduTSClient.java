/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.kududb.ts;

import com.stumbleupon.async.Deferred;

import java.util.List;
import javax.annotation.concurrent.ThreadSafe;

import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;
import org.kududb.client.AsyncKuduClient;
import org.kududb.client.KuduTable;

@InterfaceAudience.Public
@InterfaceStability.Unstable
@ThreadSafe
public class KuduTSClient implements AutoCloseable {

  private final AsyncKuduClient client;

  private KuduTSClient(List<String> masterAddresses) {
    client = new AsyncKuduClient.AsyncKuduClientBuilder(masterAddresses).build();
  }

  public static KuduTSClient create(List<String> masterAddresses) {
    return new KuduTSClient(masterAddresses);
  }

  public KuduTSTable OpenTable(String tableName) throws Exception {
    Deferred<KuduTable> metrics = client.openTable(KuduTSSchema.metricsTableName(tableName));
    Deferred<KuduTable> tagsets = client.openTable(KuduTSSchema.tagsetsTableName(tableName));
    Deferred<KuduTable> tags = client.openTable(KuduTSSchema.tagsTableName(tableName));
    KuduTable metricsTable = metrics.join(client.getDefaultAdminOperationTimeoutMs());
    KuduTable tagsetsTable = tagsets.join(client.getDefaultAdminOperationTimeoutMs());
    KuduTable tagsTable = tags.join(client.getDefaultAdminOperationTimeoutMs());

    return new KuduTSTable(client, tableName, metricsTable, tagsetsTable, tagsTable);
  }

  @Override
  public void close() throws Exception {
    client.close();
  }
}