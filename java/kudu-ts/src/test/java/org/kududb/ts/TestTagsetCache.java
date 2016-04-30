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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.stumbleupon.async.Deferred;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;

import org.junit.Test;
import org.kududb.client.BaseKuduTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTagsetCache extends BaseKuduTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestTagsetCache.class);

  @Test(timeout = 10000)
  public void testTimeseriesLookup() throws Exception {
    KuduTSClient client = KuduTSClient.create(ImmutableList.of(masterAddresses));
    KuduTSTable table = client.CreateTable("testTimeseriesLookup");
    TagsetCache cache = table.getTagsetCache();
    SortedMap<String, String> tagset = ImmutableSortedMap.of("k1", "v1");

    long tagsetHash = cache.hashSerializedTagset(TagsetCache.serializeTagset(tagset));

    long insertID = cache.getTagsetID(tagset).join();
    cache.clear();
    long lookupID = cache.getTagsetID(tagset).join();

    assertEquals(tagsetHash, insertID);
    assertEquals(insertID, lookupID);
  }

  @Test(timeout = 10000)
  public void testConcurrentLookup() throws Exception {
    KuduTSClient client = KuduTSClient.create(ImmutableList.of(masterAddresses));
    KuduTSTable table = client.CreateTable("testConcurrentLookup");
    TagsetCache cache = table.getTagsetCache();
    SortedMap<String, String> tagset = ImmutableSortedMap.of("k1", "v1");

    Deferred<Long> d1 = cache.getTagsetID(tagset);
    cache.clear();
    Deferred<Long> d2 = cache.getTagsetID(tagset);

    assertNotEquals(d1, d2);

    long id1 = d1.join();
    long id2 = d2.join();

    assertEquals(id1, id2);
  }

  @Test
  public void testOverlappingTagsets() throws Exception {
    KuduTSClient client = KuduTSClient.create(ImmutableList.of(masterAddresses));
    KuduTSTable table = client.CreateTable("testMultipleTagsets");
    TagsetCache cache = table.getTagsetCache();
    long id1 = cache.getTagsetID(ImmutableSortedMap.of("k1", "v1")).join();
    long id2 = cache.getTagsetID(ImmutableSortedMap.of("k2", "v2")).join();
    long id3 = cache.getTagsetID(ImmutableSortedMap.of("k1", "v1", "k2", "v2", "k3", "v3")).join();
    long id4 = cache.getTagsetID(ImmutableSortedMap.of("k1", "v2")).join();
    long id5 = cache.getTagsetID(ImmutableSortedMap.of("k2", "v1")).join();
    assertEquals(5, ImmutableSet.of(id1, id2, id3, id4, id5).size());
  }

  @Test
  public void testHashCollisions() throws Exception {
    KuduTSClient client = KuduTSClient.create(ImmutableList.of(masterAddresses));
    KuduTSTable table = client.CreateTable("testHashCollisions");
    TagsetCache cache = table.getTagsetCache();

    int numTagsets = 100;

    cache.setHashForTesting(Long.MAX_VALUE - 35);

    List<Deferred<Long>> deferreds = new ArrayList<>();
    for (long i = Long.MAX_VALUE - 35L; i <= Long.MAX_VALUE; i++) {
      deferreds.add(cache.getTagsetID(ImmutableSortedMap.of("key", Long.toString(i))));
    }
    for (long i = 0; i <= numTagsets; i++) {
      deferreds.add(cache.getTagsetID(ImmutableSortedMap.of("key", Long.toString(i))));
    }

    List<Long> ids = Deferred.group(deferreds).join();
    Collections.sort(ids);

    for (long i = Long.MAX_VALUE - 35L; i < numTagsets; i++) {
      assertEquals(i, ids.get((int) i).longValue());
    }
  }

  @Test
  public void testHashWraparound() throws Exception {
    KuduTSClient client = KuduTSClient.create(ImmutableList.of(masterAddresses));
    KuduTSTable table = client.CreateTable("testHashCollisions");
    TagsetCache cache = table.getTagsetCache();

    int numTagsets = 1;

    cache.setHashForTesting(Long.MAX_VALUE - 9);

    List<Deferred<Long>> deferreds = new ArrayList<>();
    for (long i = 0; i < numTagsets; i++) {
      deferreds.add(cache.getTagsetID(ImmutableSortedMap.of("key", Long.toString(i))));
    }

    List<Long> ids = Deferred.group(deferreds).join();
    Collections.sort(ids);

    for (long i = 0; i < numTagsets - 35; i++) {
      assertEquals(i, ids.get((int) i).longValue());
    }

    for (long i = numTagsets - 35; i < numTagsets; i++) {
      assertEquals(Long.MAX_VALUE - i, ids.get((int) i).longValue());
    }
  }
}
