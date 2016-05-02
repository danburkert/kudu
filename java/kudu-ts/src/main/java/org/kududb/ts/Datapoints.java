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
import com.google.common.base.Preconditions;
import com.google.common.collect.UnmodifiableIterator;
import com.stumbleupon.async.Deferred;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class Datapoints implements Iterable<Datapoint> {

  private final String metric;
  private final IntVec tagsetIDs;
  private final LongVec timestamps;
  private final DoubleVec values;

  Datapoints(String metric, IntVec tagsetIDs, LongVec timestamps, DoubleVec values) {
    Preconditions.checkArgument(tagsetIDs.len() > 0);
    Preconditions.checkArgument(timestamps.len() == values.len());
    this.metric = metric;
    this.tagsetIDs = tagsetIDs;
    this.timestamps = timestamps;
    this.values = values;
  }

  /**
   * Returns the name of the metric.
   */
  String getMetric() {
    return metric;
  }

  /**
   * Returns the tags associated with these data points.
   * @return A non-{@code null} map of tag names (keys), tag values (values).
   */
  Map<String, String> getTags() {
    try {
      return getTagsAsync().joinUninterruptibly();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
  }

  /**
   * Returns the tags associated with these data points.
   * @return A non-{@code null} map of tag names (keys), tag values (values).
   * @since 1.2
   */
  Deferred<Map<String, String>> getTagsAsync() {
    throw new RuntimeException("not implemented");
  }

  /**
   * Returns a map of tag pairs as UIDs.
   * When used on a span or row, it returns the tag set. When used on a span
   * group it will return only the tag pairs that are common across all
   * time series in the group.
   * @return A potentially empty map of tagk to tagv pairs as UIDs
   * @since 2.2
   */
  List<Integer> getTagsetIDs() {
    return Collections.unmodifiableList(tagsetIDs.asList());
  }

  /**
   * Returns the number of data points.
   */
  int size() {
    return timestamps.len();
  }

  /**
   * Returns a <em>zero-copy view</em> to go through {@code size()} data points.
   * <p>
   * The iterator returned must return each {@link Datapoint} in {@code O(1)}.
   * <b>The {@link Datapoint} returned must not be stored</b> and gets
   * invalidated as soon as {@code next} is called on the iterator.  If you
   * want to store individual data points, you need to copy the timestamp
   * and value out of each {@link Datapoint} into your own data structures.
   */
  public DatapointIterator iterator() {
    return new DatapointIterator();
  }

  long timestamp(int i) {
    return timestamps.get(i);
  }

  double value(int i) {
    return values.get(i);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("metric", metric)
        .add("series-count", tagsetIDs.len())
        .add("datapoint-count", timestamps.len())
        .toString();
  }

  /**
   * A {@link Datapoint} iterator over a {@code Datapoints}. The
   * {@code Datapoint} returned by {@link #next} is reused, so callers should
   * not hold on to it. If the {@code Datapoint}'s timestamp or value needs to
   * be saved between calls to {@link #next}, then the caller is reponsible for
   * copying them.
   */
  @NotThreadSafe
  public class DatapointIterator extends UnmodifiableIterator<Datapoint> {
    private final Datapoint datapoint = Datapoint.create(0, 0);
    private final LongVec.Iterator timestampIter = timestamps.iterator();
    private final DoubleVec.Iterator valueIter = values.iterator();

    public boolean hasNext() {
      return timestampIter.hasNext();
    }

    @Override
    public Datapoint next() {
      datapoint.setTime(timestampIter.next());
      datapoint.setValue(valueIter.next());
      return datapoint;
    }

    /**
     * Seek to the first datapoint at or after the provided time in microseconds.
     * @param microseconds the time to seek to
     */
    public void seek(long microseconds) {
      timestampIter.seekToValue(microseconds);
      valueIter.seek(timestampIter.getIndex());
    }
  }
}
