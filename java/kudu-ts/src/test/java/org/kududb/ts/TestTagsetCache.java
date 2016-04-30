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
  public void testTagsetLookup() throws Exception {
    KuduTSClient client = KuduTSClient.create(ImmutableList.of(masterAddresses));
    KuduTSTable table = client.CreateTable("testTagsetLookup");
    TagsetCache cache = table.getTagsetCache();
    SortedMap<String, String> tagset = ImmutableSortedMap.of("k1", "v1");

    int tagsetHash = cache.hashSerializedTagset(TagsetCache.serializeTagset(tagset));

    int insertID = cache.getTagsetID(tagset).join();
    cache.clear();
    int lookupID = cache.getTagsetID(tagset).join();

    assertEquals(tagsetHash, insertID);
    assertEquals(insertID, lookupID);
  }

  @Test(timeout = 10000)
  public void testConcurrentLookup() throws Exception {
    KuduTSClient client = KuduTSClient.create(ImmutableList.of(masterAddresses));
    KuduTSTable table = client.CreateTable("testConcurrentLookup");
    TagsetCache cache = table.getTagsetCache();
    SortedMap<String, String> tagset = ImmutableSortedMap.of("k1", "v1");

    List<Deferred<Integer>> deferreds = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      deferreds.add(cache.getTagsetID(tagset));
      cache.clear();
    }

    assertEquals(1, ImmutableSet.copyOf(Deferred.group(deferreds).join()).size());
  }

  @Test(timeout = 10000)
  public void testEmptyTagsetLookup() throws Exception {
    KuduTSClient client = KuduTSClient.create(ImmutableList.of(masterAddresses));
    KuduTSTable table = client.CreateTable("testEmptyTagsetLookup");
    TagsetCache cache = table.getTagsetCache();
    int id = cache.getTagsetID(ImmutableSortedMap.<String, String>of()).join();
    assertEquals(0, id);
  }

  @Test(timeout = 10000)
  public void testHashWraparound() throws Exception {
    KuduTSClient client = KuduTSClient.create(ImmutableList.of(masterAddresses));
    KuduTSTable table = client.CreateTable("testHashWraparound");
    TagsetCache cache = table.getTagsetCache();

    cache.setHashForTesting(Integer.MAX_VALUE - 9);

    int id = cache.getTagsetID(ImmutableSortedMap.of("key", "val")).join();
    assertEquals(Integer.MAX_VALUE - 9, id);
  }

  @Test(timeout = 10000)
  public void testOverlappingTagsets() throws Exception {
    KuduTSClient client = KuduTSClient.create(ImmutableList.of(masterAddresses));
    KuduTSTable table = client.CreateTable("testOverlappingTagsets");
    TagsetCache cache = table.getTagsetCache();
    int id1 = cache.getTagsetID(ImmutableSortedMap.of("k1", "v1")).join();
    int id2 = cache.getTagsetID(ImmutableSortedMap.of("k2", "v2")).join();
    int id3 = cache.getTagsetID(ImmutableSortedMap.of("k1", "v1", "k2", "v2", "k3", "v3")).join();
    int id4 = cache.getTagsetID(ImmutableSortedMap.of("k1", "v2")).join();
    int id5 = cache.getTagsetID(ImmutableSortedMap.of("k2", "v1")).join();
    int id6 = cache.getTagsetID(ImmutableSortedMap.<String, String>of()).join();
    assertEquals(6, ImmutableSet.of(id1, id2, id3, id4, id5, id6).size());
  }

  @Test(timeout = 10000)
  public void testHashCollisions() throws Exception {
    KuduTSClient client = KuduTSClient.create(ImmutableList.of(masterAddresses));
    KuduTSTable table = client.CreateTable("testHashCollisions");
    TagsetCache cache = table.getTagsetCache();

    int numTagsets = 100;
    cache.setHashForTesting(0);

    List<Deferred<Integer>> deferreds = new ArrayList<>();
    for (int i = 0; i <= numTagsets; i++) {
      deferreds.add(cache.getTagsetID(ImmutableSortedMap.of("key", Integer.toString(i))));
    }

    List<Integer> ids = Deferred.group(deferreds).join();
    Collections.sort(ids);

    for (int i = 0; i < numTagsets; i++) {
      assertEquals(i, ids.get(i).intValue());
    }
  }

  @Test(timeout = 10000)
  public void testHashCollisionsWraparound() throws Exception {
    KuduTSClient client = KuduTSClient.create(ImmutableList.of(masterAddresses));
    KuduTSTable table = client.CreateTable("testHashCollisionsWraparound");
    TagsetCache cache = table.getTagsetCache();

    int numTagsets = 30;
    int offset = 15;

    cache.setHashForTesting(Integer.MAX_VALUE - offset);

    List<Deferred<Integer>> deferreds = new ArrayList<>();
    for (int i = 0; i < numTagsets; i++) {
      deferreds.add(cache.getTagsetID(ImmutableSortedMap.of("key", Integer.toString(i))));
    }

    List<Integer> ids = Deferred.group(deferreds).join();
    Collections.sort(ids);

    List<Integer> expectedIds = new ArrayList<>();
    for (int i = 0; i < numTagsets; i++) {
      expectedIds.add((Integer.MAX_VALUE - offset) + i);
    }
    Collections.sort(expectedIds);
    assertEquals(expectedIds, ids);
  }

  @Test(timeout = 10000)
  public void testSaturatingAdd() throws Exception {
    assertEquals(10, TagsetCache.saturatingAdd(10, 0));
    assertEquals(10, TagsetCache.saturatingAdd(0, 10));
    assertEquals(10, TagsetCache.saturatingAdd(5, 5));
    assertEquals(10, TagsetCache.saturatingAdd(-15, 25));
    assertEquals(10, TagsetCache.saturatingAdd(150, -140));

    assertEquals(Integer.MAX_VALUE, TagsetCache.saturatingAdd(Integer.MAX_VALUE, 22));
    assertEquals(Integer.MAX_VALUE - 22, TagsetCache.saturatingAdd(Integer.MAX_VALUE, -22 ));
    assertEquals(Integer.MAX_VALUE, TagsetCache.saturatingAdd(Integer.MAX_VALUE - 35, 35));
    assertEquals(Integer.MAX_VALUE, TagsetCache.saturatingAdd(Integer.MAX_VALUE - 35, 36));
    assertEquals(Integer.MAX_VALUE, TagsetCache.saturatingAdd(Integer.MAX_VALUE, Integer.MAX_VALUE));
  }
}
