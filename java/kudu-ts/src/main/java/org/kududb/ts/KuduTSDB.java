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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import java.util.List;
import javax.annotation.concurrent.ThreadSafe;

import org.kududb.Schema;
import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;
import org.kududb.client.AsyncKuduClient;
import org.kududb.client.KuduTable;
import org.kududb.client.MasterErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Public
@InterfaceStability.Unstable
@ThreadSafe
public class KuduTSDB implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(KuduTSDB.class);

  private final AsyncKuduClient client;
  private String databaseName;

  private final KuduTable metricsTable;
  private final KuduTable tagsetsTable;
  private final KuduTable tagsTable;

  private final Tags tags;
  private final Tagsets tagsets;

  private KuduTSDB(AsyncKuduClient client,
                   String databaseName,
                   KuduTable metricsTable,
                   KuduTable tagsetsTable,
                   KuduTable tagsTable,
                   Tags tags,
                   Tagsets tagsets) {
    this.client = client;
    this.databaseName = databaseName;
    this.metricsTable = metricsTable;
    this.tagsetsTable = tagsetsTable;
    this.tagsTable = tagsTable;
    this.tags = tags;
    this.tagsets = tagsets;
  }

  public static KuduTSDB open(List<String> kuduMasterAddressess, String databaseName) throws Exception {
    AsyncKuduClient client = new AsyncKuduClient.AsyncKuduClientBuilder(kuduMasterAddressess).build();

    Deferred<KuduTable> metricsDeferred = openOrCreateTable(client, KuduTSSchema.metricsTableName(databaseName), KuduTSSchema.METRICS_SCHEMA);
    Deferred<KuduTable> tagsetsDeferred = openOrCreateTable(client, KuduTSSchema.tagsetsTableName(databaseName), KuduTSSchema.TAGSETS_SCHEMA);
    Deferred<KuduTable> tagsDeferred = openOrCreateTable(client, KuduTSSchema.tagsTableName(databaseName), KuduTSSchema.TAGS_SCHEMA);
    KuduTable metricsTable = metricsDeferred.join(client.getDefaultAdminOperationTimeoutMs());
    KuduTable tagsetsTable = tagsetsDeferred.join(client.getDefaultAdminOperationTimeoutMs());
    KuduTable tagsTable = tagsDeferred.join(client.getDefaultAdminOperationTimeoutMs());

    Tags tags = new Tags(client, tagsTable);
    Tagsets tagsets = new Tagsets(client, tags, tagsetsTable);
    return new KuduTSDB(client, databaseName, metricsTable, tagsetsTable, tagsTable, tags, tagsets);
  }

  private static Deferred<KuduTable> openOrCreateTable(final AsyncKuduClient client,
                                                       final String table,
                                                       final Schema schema) throws Exception {
    class CreateTableErrback implements Callback<Deferred<KuduTable>, Exception> {
      @Override
      public Deferred<KuduTable> call(Exception e) throws Exception {
        if (e instanceof MasterErrorException) {
          LOG.debug("Creating table {}", table);
          return client.createTable(table, schema);
        } else {
          throw e;
        }
      }
      @Override
      public String toString() {
        return Objects.toStringHelper(this).add("table", table).toString();
      }
    }

    return client.openTable(table).addErrback(new CreateTableErrback());
  }

  @VisibleForTesting
  Tags getTags() {
    return tags;
  }

  @VisibleForTesting
  Tagsets getTagsets() {
    return tagsets;
  }

  @Override
  public void close() throws Exception {
    client.close();
  }
}