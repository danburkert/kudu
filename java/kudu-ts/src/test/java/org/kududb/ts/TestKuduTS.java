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
import com.google.common.collect.ImmutableSortedMap;

import java.util.SortedMap;

import org.junit.Test;
import org.kududb.client.AsyncKuduClient;
import org.kududb.client.BaseKuduTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestKuduTS extends BaseKuduTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestKuduTS.class);

  @Test
  public void testWriteMetric() throws Exception {
    KuduTSClient client = KuduTSClient.create(ImmutableList.of(getMasterAddresses()));
//    KuduTSTable table = client.CreateTable("testWriteMetric");
//    AsyncKuduClient kuduClient = table.getClient();
  }

  @Test(timeout = 100000)
  public void test() throws Exception {
    KuduTSClient client = KuduTSClient.create(ImmutableList.of(getMasterAddresses()));

//    KuduTSTable table = client.CreateTable("test");
//
//    SortedMap<String, String> tags = ImmutableSortedMap.of("host", "localhost", "dc", "oregon");
//    for (int i = 0; i < 10; i++) {
//      table.writeMetric("test", tags, i, i);
//    }
//    table.flush();
//
//    QueryResult qr = table.queryMetrics(-1, 11, "test", tags);
//    assertEquals(10, qr.getDatapoints().size());
//
//    tags = ImmutableSortedMap.of("host", "otherhost", "dc", "oregon");
//    for (int i = 0; i < 10; i += 2) {
//      table.writeMetric("test", tags, i, i * 2);
//    }
//    table.flush();
//
//    tags = ImmutableSortedMap.of("dc", "orgeon");
//    qr = table.queryMetrics(0, 11, "test", tags);
//    assertEquals(10, qr.getDatapoints().size());

    /*
    tags = ImmutableSortedMap.of("host", "localhost");
    qr = table.queryMetrics(-1, 11, "test", tags);
    assertEquals(5, qr.getDatapoints().size());
    */
  }
}
