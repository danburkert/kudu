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
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;
import org.kududb.client.AsyncKuduClient;
import org.kududb.client.AsyncKuduScanner;
import org.kududb.client.AsyncKuduSession;
import org.kududb.client.Insert;
import org.kududb.client.KuduPredicate;
import org.kududb.client.KuduTable;
import org.kududb.client.OperationResponse;
import org.kududb.client.PartialRow;
import org.kududb.client.PleaseThrottleException;
import org.kududb.client.RowResult;
import org.kududb.client.RowResultIterator;
import org.kududb.client.SessionConfiguration;
import org.kududb.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

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
  private final AsyncKuduSession session;

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
    this.metricsQueryProjection = Lists.newArrayList(2, 3); // time, value
    this.tagsQueryProjection = Lists.newArrayList(2); // tagset_id
    this.session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
  }

  public void writeMetric(String metricName, SortedMap<String, String> tags,
                          long timestamp, double value) throws Exception {
    long id = tagsetCache.getTagsetID(tags).join(10000);
    Insert insert = metricsTable.newInsert();
    PartialRow row = insert.getRow();
    row.addString(0, metricName);
    row.addLong(1, id);
    row.addLong(2, timestamp);
    row.addDouble(3, value);
    while (true) {
      try {
        session.apply(insert);
        break;
      } catch (PleaseThrottleException ex) {
        ex.getDeferred().join(10000);
      }
    }
  }

  public void flush() throws Exception {
    session.flush().join(10000);
  }

  public QueryResult queryMetrics(long startTimestampMs,
                                  long endTimestampMs,
                                  String metricName,
                                  SortedMap<String, String> tags) throws Exception {

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

    List<TimeAndValue> dataPoints =
        getDataPoints(startTimestampMs, endTimestampMs, metricName, tagsetIDs);

    qr.setTags(tags);
    qr.setDatapoints(dataPoints);

    // TODO need to grab all the tags for each tagsets?

    return qr;
  }

  private List<TimeAndValue> getDataPoints(long startTimestampMs,
                             long endTimestampMs,
                             String metricName,
                             Set<Long> tagsetIDs) throws Exception {
    List<Deferred<PeekingIterator<Pair<Long, Double>>>> deferreds = new ArrayList<>(tagsetIDs.size());

    // Launch scanners for all the tagsetIDs.
    for (Long tagsetId : tagsetIDs) {
      KuduPredicate metricPred = KuduPredicate.newComparisonPredicate(
          metricsTable.getSchema().getColumnByIndex(0),
          KuduPredicate.ComparisonOp.EQUAL, metricName);

      KuduPredicate tagsetIdPred = KuduPredicate.newComparisonPredicate(
          metricsTable.getSchema().getColumnByIndex(1),
          KuduPredicate.ComparisonOp.EQUAL, tagsetId);

      KuduPredicate startTimestampPred = KuduPredicate.newComparisonPredicate(
          metricsTable.getSchema().getColumnByIndex(2),
          KuduPredicate.ComparisonOp.GREATER, startTimestampMs);

      KuduPredicate endTimestampPred = KuduPredicate.newComparisonPredicate(
          metricsTable.getSchema().getColumnByIndex(2),
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

    List<PeekingIterator<Pair<Long, Double>>> iterators = Deferred.group(deferreds).join(10000);
    List<TimeAndValue> finalDataPoints = new ArrayList<>();


    while (!iterators.isEmpty()) {
      long lowestValue = Long.MAX_VALUE;
      List<PeekingIterator<Pair<Long, Double>>> iteratorsToNextOn = new ArrayList<>(iterators.size());
      Iterator<PeekingIterator<Pair<Long, Double>>> iteratorOfIterators = iterators.iterator();

      // First peek all the iterators, find the lowest timestamps and keep a list of the iterators
      // that have it.
      while (iteratorOfIterators.hasNext()) {
        PeekingIterator<Pair<Long, Double>> iterator = iteratorOfIterators.next();
        if (!iterator.hasNext()) {
          iteratorOfIterators.remove();
          continue;
        }

        long currentVal = iterator.peek().getFirst();
        if (currentVal < lowestValue) {
          iteratorsToNextOn.clear();
          lowestValue = currentVal;
          iteratorsToNextOn.add(iterator);
        } else if (currentVal == lowestValue) {
          iteratorsToNextOn.add(iterator);
        }
      }

      if (lowestValue == Long.MAX_VALUE) {
        assert (iterators.isEmpty());
        continue;
      }

      // TODO we'd normally call an aggregation function here.
      // Right now we average.
      double sum = 0.0;
      for (PeekingIterator<Pair<Long, Double>> iterator : iteratorsToNextOn) {
        sum += iterator.next().getSecond();
      }
      finalDataPoints.add(new TimeAndValue(lowestValue, sum / iteratorsToNextOn.size()));
    }

    return finalDataPoints;
  }

  private class MetricsScannerCB implements
      Callback<Deferred<PeekingIterator<Pair<Long, Double>>>, RowResultIterator> {

    private final AsyncKuduScanner metricsScanner;
    private final List<Pair<Long, Double>> datapoints = new ArrayList<>();

    MetricsScannerCB(AsyncKuduScanner metricsScanner) {
      this.metricsScanner = metricsScanner;
    }

    @Override
    public Deferred<PeekingIterator<Pair<Long, Double>>> call(RowResultIterator rowResults) throws Exception {

      for (RowResult rr : rowResults) {
        datapoints.add(new Pair<Long, Double>(rr.getLong(0), rr.getDouble(1)));
      }

      if (metricsScanner.hasMoreRows()) {
        return metricsScanner.nextRows().addCallbackDeferring(this);
      }
      PeekingIterator<Pair<Long, Double>> iterator = Iterators.peekingIterator(datapoints.iterator());
      return Deferred.fromResult(iterator);
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
