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

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

import org.kududb.client.AsyncKuduClient;
import org.kududb.client.AsyncKuduScanner;
import org.kududb.client.AsyncKuduSession;
import org.kududb.client.Insert;
import org.kududb.client.KuduPredicate;
import org.kududb.client.KuduTable;
import org.kududb.client.OperationResponse;
import org.kududb.client.RowResult;
import org.kududb.client.RowResultIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code Metrics} manages inserting and retrieving datapoints from the
 * {@code metrics} table.
 */
public class Metrics {
  private static final Logger LOG = LoggerFactory.getLogger(Metrics.class);

  private static final List<Integer> TIME_VALUE_PROJECTION =
      ImmutableList.of(KuduTSSchema.METRICS_TIME_INDEX,
                       KuduTSSchema.METRICS_VALUE_INDEX);

  private final AsyncKuduClient client;
  private final KuduTable table;
  private final Tagsets tagsets;

  public Metrics(AsyncKuduClient client, KuduTable table, Tagsets tagsets) {
    this.client = client;
    this.table = table;
    this.tagsets = tagsets;
  }

  public Deferred<ArrayList<OperationResponse>> insertDataPoints(final AsyncKuduSession session,
                                                                 final String metric,
                                                                 final SortedMap<String, String> tagset,
                                                                 final List<Datapoint> datapoints) {

    class TagsetIDLookupCB implements Callback<Deferred<ArrayList<OperationResponse>>, Integer> {
      @Override
      public Deferred<ArrayList<OperationResponse>> call(Integer tagsetID) throws Exception {
        List<Deferred<OperationResponse>> responses = new ArrayList<>(datapoints.size());
        for (Datapoint dataPoint : datapoints) {
          Insert insert = table.newInsert();
          insert.getRow().addString(KuduTSSchema.METRICS_METRIC_INDEX, metric);
          insert.getRow().addInt(KuduTSSchema.METRICS_TAGSET_ID_INDEX, tagsetID);
          insert.getRow().addLong(KuduTSSchema.METRICS_TIME_INDEX, dataPoint.getTime());
          insert.getRow().addDouble(KuduTSSchema.METRICS_VALUE_INDEX, dataPoint.getValue());
          responses.add(session.apply(insert));
        }
        return Deferred.group(responses);
      }

      @Override
      public String toString() {
        return Objects.toStringHelper(this)
                      .add("metric", metric)
                      .add("tags", tagset)
                      .add("datapoint-count", datapoints.size())
                      .toString();
      }
    }

    return tagsets.getTagsetID(tagset).addCallbackDeferring(new TagsetIDLookupCB());
  }

  public Deferred<Datapoints> getSeries(final AsyncKuduSession session,
                                        final String metric,
                                        final SortedMap<String, String> tagset) {
    return null;
  }

  public Deferred<Datapoints> getSeries(final String metric,
                                        final int tagsetID,
                                        final long startTime,
                                        final long endTime) {


    KuduPredicate metricPred =
        KuduPredicate.newComparisonPredicate(KuduTSSchema.METRICS_METRIC_COLUMN,
                                             KuduPredicate.ComparisonOp.EQUAL, metric);

    KuduPredicate tagsetIdPred =
        KuduPredicate.newComparisonPredicate(KuduTSSchema.METRICS_TAGSET_ID_COLUMN,
                                             KuduPredicate.ComparisonOp.EQUAL, tagsetID);

    KuduPredicate startTimestampPred =
        KuduPredicate.newComparisonPredicate(KuduTSSchema.METRICS_TIME_COLUMN,
                                             KuduPredicate.ComparisonOp.GREATER_EQUAL, startTime);

    KuduPredicate endTimestampPred =
        KuduPredicate.newComparisonPredicate(KuduTSSchema.METRICS_TIME_COLUMN,
                                             KuduPredicate.ComparisonOp.LESS, endTime);

    final AsyncKuduScanner scanner = client.newScannerBuilder(table)
                                           .addPredicate(metricPred)
                                           .addPredicate(tagsetIdPred)
                                           .addPredicate(startTimestampPred)
                                           .addPredicate(endTimestampPred)
                                           .setProjectedColumnIndexes(TIME_VALUE_PROJECTION)
                                           .build();

    class GetSeriesCB implements Callback<Deferred<Datapoints>, RowResultIterator> {
      private final LongVec times = LongVec.create();
      private final DoubleVec values = DoubleVec.create();
      @Override
      public Deferred<Datapoints> call(RowResultIterator results) throws Exception {
        times.reserve(results.getNumRows());
        values.reserve(results.getNumRows());
        for (RowResult result : results) {
          times.push(result.getLong(0));
          values.push(result.getDouble(1));
        }

        if (scanner.hasMoreRows()) {
          return scanner.nextRows().addCallbackDeferring(this);
        }
        return Deferred.fromResult(new Datapoints(metric, IntVec.))



      }
    }


    return scanner.nextRows().addCallbackDeferring(new GetSeriesCB());
  }
}
