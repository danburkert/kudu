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

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;
import org.kududb.client.AsyncKuduClient;
import org.kududb.client.AsyncKuduScanner;
import org.kududb.client.KuduPredicate;
import org.kududb.client.KuduTable;
import org.kududb.client.RowResult;
import org.kududb.client.RowResultIterator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@InterfaceAudience.Public
@InterfaceStability.Evolving
@NotThreadSafe
public class KuduTSTable {

  private final String tableName;
  private final KuduTSSchema schema;

  private final AsyncKuduClient client;
  private final KuduTable metricsTable;
  private final KuduTable tagsetsTable;
  private final KuduTable tagsTable;

  private final TagsetCache tagsetCache;
  private final List<Integer> metricsQueryProjection;
  private final List<Integer> tagsQueryProjection;

  public String getTableName() {
    return tableName;
  }

  public KuduTSSchema getSchema() {
    return schema;
  }

  @VisibleForTesting
  public TagsetCache getTagsetCache() {
    return tagsetCache;
  }

  KuduTSTable(AsyncKuduClient client,
              String tableName,
              KuduTSSchema schema,
              KuduTable metricsTable,
              KuduTable tagsetsTable,
              KuduTable tagsTable) {
    this.client = client;
    this.tableName = tableName;
    this.schema = schema;
    this.metricsTable = metricsTable;
    this.tagsetsTable = tagsetsTable;
    this.tagsTable = tagsTable;
    this.tagsetCache = new TagsetCache(client, schema, tagsetsTable, tagsTable);
    this.metricsQueryProjection = Lists.newArrayList(2, 3);
    this.tagsQueryProjection = Lists.newArrayList(2);
  }

  public QueryResult queryMetrics(long startTimestampMs,
                                  long endTimestampMs,
                                  String metricName,
                                  Map<String, String> tags) throws Exception {

    QueryResult qr  = new QueryResult(metricName);
    // If the user doesn't provide tags, then we want all the data points for the specified metric.
    Set<Long> tagsetIDs;
    if (tags.isEmpty()) {
      tagsetIDs = new HashSet<>();
    } else {
      tagsetIDs = getTagsetIdsForTags(tags);
      if (tagsetIDs.isEmpty()) {
        // We know we won't find any metrics.
        return qr;
      }
    }

    List<Deferred<Void>> deferreds = new ArrayList<>(tagsetIDs.size());

    // Launch scanners for all the tagsetIDs.
    for (Long tagsetId : tagsetIDs) {
      KuduPredicate metricPred = KuduPredicate.newComparisonPredicate(
          tagsTable.getSchema().getColumnByIndex(0),
          KuduPredicate.ComparisonOp.EQUAL, metricName);

      KuduPredicate tagsetIdPred = KuduPredicate.newComparisonPredicate(
          tagsTable.getSchema().getColumnByIndex(1),
          KuduPredicate.ComparisonOp.EQUAL, tagsetId);

      KuduPredicate startTimestampPred = KuduPredicate.newComparisonPredicate(
          tagsTable.getSchema().getColumnByIndex(2),
          KuduPredicate.ComparisonOp.GREATER, startTimestampMs);

      KuduPredicate endTimestampPred = KuduPredicate.newComparisonPredicate(
          tagsTable.getSchema().getColumnByIndex(2),
          KuduPredicate.ComparisonOp.LESS, endTimestampMs);

      AsyncKuduScanner metricScanner = client.newScannerBuilder(metricsTable)
          .addPredicate(metricPred)
          .addPredicate(tagsetIdPred)
          .addPredicate(startTimestampPred)
          .addPredicate(endTimestampPred)
          .setProjectedColumnIndexes(metricsQueryProjection)
          .build();

      deferreds.add(metricScanner.nextRows().addCallbackDeferring(
          new MetricsScannerCB(metricScanner)));
    }

    // TODO need to grab all the tags for each tagsets?

    return qr;
  }

  private class MetricsScannerCB implements Callback<Deferred<Void>, RowResultIterator> {

    private final AsyncKuduScanner metricsScanner;

    MetricsScannerCB(AsyncKuduScanner metricsScanner) {
      this.metricsScanner = metricsScanner;
    }

    @Override
    public Deferred<Void> call(RowResultIterator rowResults) throws Exception {

      for (RowResult rr : rowResults) {
        //ids.add(rr.getLong(0));
      }

      if (metricsScanner.hasMoreRows()) {
        return metricsScanner.nextRows().addCallbackDeferring(this);
      }
      return Deferred.fromResult(null);
    }
  }

  private Set<Long> getTagsetIdsForTags(Map<String, String> tags) throws Exception {
    Preconditions.checkArgument(!tags.isEmpty());

    List<Deferred<Set<Long>>> deferreds = new ArrayList<>(tags.size());

    // Kickoff all the scanners and add them to deferreds.
    for ( Map.Entry<String, String> entry : tags.entrySet()) {
      KuduPredicate keyPred = KuduPredicate.newComparisonPredicate(
          tagsTable.getSchema().getColumnByIndex(0),
          KuduPredicate.ComparisonOp.EQUAL, entry.getKey());

      KuduPredicate valuePred = KuduPredicate.newComparisonPredicate(
          tagsTable.getSchema().getColumnByIndex(1),
          KuduPredicate.ComparisonOp.EQUAL, entry.getValue());

      AsyncKuduScanner tagsScanner = client.newScannerBuilder(tagsTable)
          .addPredicate(keyPred)
          .addPredicate(valuePred)
          .setProjectedColumnIndexes(tagsQueryProjection)
          .build();

      deferreds.add(tagsScanner.nextRows().addCallbackDeferring(new TagsScannerCB(tagsScanner)));
    }

    // Do a group wait, collect all the results.
    ArrayList<Set<Long>> result = Deferred.group(deferreds).join(10000);

    // Get the intersection of all the tags.
    Set<Long> intersection = new HashSet<>(result.get(0));
    for (Set<Long> ids : result) {
      intersection.retainAll(ids);

      // Short-circuit, nothing else is going to happen.
      if (ids.isEmpty()) {
        break;
      }
    }

    return intersection;
  }

  private class TagsScannerCB implements Callback<Deferred<Set<Long>>, RowResultIterator> {
    private final Set<Long> ids = new HashSet<>();
    private final AsyncKuduScanner tagsScanner;

    TagsScannerCB(AsyncKuduScanner tagsScanner) {
      this.tagsScanner = tagsScanner;
    }

    @Override
    public Deferred<Set<Long>> call(RowResultIterator rowResults) throws Exception {
      for (RowResult rr : rowResults) {
        ids.add(rr.getLong(0));
      }
      if (tagsScanner.hasMoreRows()) {
        return tagsScanner.nextRows().addCallbackDeferring(this);
      }
      return Deferred.fromResult(ids);
    }
  }
}
