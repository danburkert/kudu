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

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;
import org.kududb.client.KuduClient;
import org.kududb.client.KuduTable;

@InterfaceAudience.Public
@InterfaceStability.Unstable
@ThreadSafe
public class KuduTSClient implements AutoCloseable {

  private final KuduClient client;

  private KuduTSClient(List<String> masterAddresses) {
    client = new KuduClient.KuduClientBuilder(masterAddresses).build();
  }

  public static KuduTSClient create(List<String> masterAddresses) {
    return new KuduTSClient(masterAddresses);
  }

  public KuduTSTable CreateTable(String tableName) throws Exception {
    KuduTSSchema schema = KuduTSSchema.create(tableName);

    KuduTable metricsTable = client.createTable(schema.getMetricsTableName(), schema.getMetricsSchema());
    KuduTable tagsetsTable = client.createTable(schema.getTagsetsTableName(), schema.getTagsetsSchema());
    KuduTable tagsTable = client.createTable(schema.getTagsTableName(), schema.getTagsSchema());

    return new KuduTSTable(tableName, schema, metricsTable, tagsetsTable, tagsTable);
  }

  public KuduTSTable OpenTable(String tableName) throws Exception {
    KuduTable metricsTable = client.openTable(KuduTSSchema.metricsTableName(tableName));
    KuduTable tagsetsTable = client.openTable(KuduTSSchema.tagsetsTableName(tableName));
    KuduTable tagsTable = client.openTable(KuduTSSchema.tagsTableName(tableName));

    KuduTSSchema schema = new KuduTSSchema(tableName,
                                           metricsTable.getSchema(),
                                           tagsetsTable.getSchema(),
                                           tagsTable.getSchema());
    return new KuduTSTable(tableName, schema, metricsTable, tagsetsTable, tagsTable);
  }

  @Override
  public void close() throws Exception {
    client.close();
  }
}