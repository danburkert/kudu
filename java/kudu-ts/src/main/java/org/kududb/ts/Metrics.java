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

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import org.kududb.annotations.InterfaceAudience;
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
@InterfaceAudience.Private
@ThreadSafe
public class Metrics {
  private static final Logger LOG = LoggerFactory.getLogger(Metrics.class);

  private static final List<Integer> TIME_VALUE_PROJECTION =
      ImmutableList.of(Tables.METRICS_TIME_INDEX,
                       Tables.METRICS_VALUE_INDEX);

  private final AsyncKuduClient client;
  private final KuduTable table;
  private final Tagsets tagsets;

  public Metrics(AsyncKuduClient client, KuduTable table, Tagsets tagsets) {
    this.client = client;
    this.table = table;
    this.tagsets = tagsets;
  }

  public Insert insertDatapoint(final String metric,
                                final int tagsetID,
                                final long time,
                                final double value) {
    Insert insert = table.newInsert();
    insert.getRow().addString(Tables.METRICS_METRIC_INDEX, metric);
    insert.getRow().addInt(Tables.METRICS_TAGSET_ID_INDEX, tagsetID);
    insert.getRow().addLong(Tables.METRICS_TIME_INDEX, time);
    insert.getRow().addDouble(Tables.METRICS_VALUE_INDEX, value);
    return insert;
  }

  public Deferred<Datapoints> scanSeries(final String metric,
                                         final int tagsetID,
                                         final long startTime,
                                         final long endTime,
                                         final Aggregator downsampler,
                                         final long downsampleInterval) {
    KuduPredicate metricPred =
        KuduPredicate.newComparisonPredicate(Tables.METRICS_METRIC_COLUMN,
                                             KuduPredicate.ComparisonOp.EQUAL, metric);

    KuduPredicate tagsetIdPred =
        KuduPredicate.newComparisonPredicate(Tables.METRICS_TAGSET_ID_COLUMN,
                                             KuduPredicate.ComparisonOp.EQUAL, tagsetID);

    KuduPredicate startTimestampPred =
        KuduPredicate.newComparisonPredicate(Tables.METRICS_TIME_COLUMN,
                                             KuduPredicate.ComparisonOp.GREATER_EQUAL, startTime);

    KuduPredicate endTimestampPred =
        KuduPredicate.newComparisonPredicate(Tables.METRICS_TIME_COLUMN,
                                             KuduPredicate.ComparisonOp.LESS, endTime);

    final AsyncKuduScanner scanner = client.newScannerBuilder(table)
                                           .addPredicate(metricPred)
                                           .addPredicate(tagsetIdPred)
                                           .addPredicate(startTimestampPred)
                                           .addPredicate(endTimestampPred)
                                           .setProjectedColumnIndexes(TIME_VALUE_PROJECTION)
                                           .build();

    class SeriesScanCB implements Callback<Deferred<Datapoints>, RowResultIterator> {
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

        return Deferred.fromResult(new Datapoints(metric,
                                                  IntVec.wrap(new int[] { tagsetID }),
                                                  times,
                                                  values));
      }

      @Override
      public String toString() {
        return Objects.toStringHelper(this)
                      .add("datapoints-count", times.len())
                      .toString();
      }
    }

    class DownsamplingSeriesScanCB implements Callback<Deferred<Datapoints>, RowResultIterator> {
      private final LongVec times = LongVec.create();
      private final DoubleVec values = DoubleVec.create();

      private long currentInterval = 0;
      private final DoubleVec intervalValues = DoubleVec.create();

      @Override
      public Deferred<Datapoints> call(RowResultIterator results) throws Exception {
        for (RowResult result : results) {
          long time = result.getLong(0);
          double value = result.getDouble(1);
          long interval = time - (time % downsampleInterval);
          if (interval == currentInterval) {
            intervalValues.push(value);
          } else {
            if (!intervalValues.isEmpty()) {
              times.push(currentInterval);
              values.concat(intervalValues);
              intervalValues.clear();
            }
            currentInterval = interval;
          }
        }

        if (scanner.hasMoreRows()) {
          return scanner.nextRows().addCallbackDeferring(this);
        }

        if (!intervalValues.isEmpty()) {
          times.push(currentInterval);
          values.concat(intervalValues);
          intervalValues.clear();
        }

        return Deferred.fromResult(new Datapoints(metric,
                                                  IntVec.wrap(new int[] { tagsetID }),
                                                  times,
                                                  values));
      }
      @Override
      public String toString() {
        return Objects.toStringHelper(this)
                      .add("datapoints-count", times.len() + (intervalValues.isEmpty() ? 0 : 1))
                      .toString();
      }
    }

    if (downsampler == null) {
      return scanner.nextRows().addCallbackDeferring(new SeriesScanCB());
    } else {
      return scanner.nextRows().addCallbackDeferring(new DownsamplingSeriesScanCB());
    }
  }
}
