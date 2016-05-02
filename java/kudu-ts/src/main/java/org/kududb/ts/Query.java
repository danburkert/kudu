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

import com.google.common.collect.ImmutableSortedMap;
import com.stumbleupon.async.Deferred;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;

/**
 * A query to retrieve timeseries data from Kudu.
 */
public class Query {

  long start;
  long end;

  String metric;

  Map<String, List<String>> tags;
  List<String> groupBys;
  SortedMap<String, List<String>> filters;

  Aggregator aggregator;

  long downsampleInterval;
  Aggregator downsampler;
  FillPolicy fillPolicy;

  void setStartTime(long microseconds) {
    start = microseconds;
  }

  void setEndTime(long microseconds) {
    end = microseconds;
  }

  long getStartTime() {
    return start;
  }

  public long getEndTime() {
    return end;
  }

  public void setMetric(String metric) {
    this.metric = metric;
  }

  public String getMetric() {
    return metric;
  }

  public void setFilters(Map<String, List<String>> tags) {
//    tagFilters = ImmutableSortedMap.copyOf(tags);
  }

  public void setTagGroups(Map<String, List<String>> tags) {
//    tagGroups = ImmutableSortedMap.copyOf(tags);
  }

  public void setAggregator(Aggregator aggregator) {

  }

  public void downsample(long interval, Aggregator downsampler, FillPolicy fillPolicy) {
    this.downsampleInterval = interval;
    this.downsampler = downsampler;
    this.fillPolicy = fillPolicy;
  }

  /**
   * Runs this query.
   */
  List<Datapoints> run() {
    throw new RuntimeException("not implemented");
  }

  /**
   * Executes the query asynchronously
   */
  public Deferred<List<Datapoints>> runAsync() {
    throw new RuntimeException("not implemented");
  };
}
