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

import javax.annotation.concurrent.NotThreadSafe;

import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;
import org.kududb.client.KuduTable;

@InterfaceAudience.Public
@InterfaceStability.Evolving
@NotThreadSafe
public class KuduTSTable {

  private final String tableName;
  private final KuduTSSchema schema;

  private final KuduTable metricsTable;
  private final KuduTable tagsetsTable;
  private final KuduTable tagsTable;

  public String getTableName() {
    return tableName;
  }

  public KuduTSSchema getSchema() {
    return schema;
  }

  KuduTSTable(String tableName,
              KuduTSSchema schema,
              KuduTable metricsTable,
              KuduTable tagsetsTable,
              KuduTable tagsTable) {
    this.tableName = tableName;
    this.schema = schema;
    this.metricsTable = metricsTable;
    this.tagsetsTable = tagsetsTable;
    this.tagsTable = tagsTable;
  }
}
