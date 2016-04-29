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

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.kududb.client.BaseKuduTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.SortedMap;
import java.util.TreeMap;

public class TestKuduTS extends BaseKuduTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestKuduTS.class);

  @Test(timeout = 100000)
  public void test() throws Exception {
    KuduTSClient client = KuduTSClient.create(Lists.<String>newArrayList(getMasterAddresses()));

    KuduTSTable table = client.CreateTable("test");

    SortedMap<String, String> tags = new TreeMap<>();
    tags.put("host", "localhost");
    tags.put("dc", "oregon");
    for (int i = 0; i < 10; i++) {
      table.writeMetric("test", tags, i, i);
    }
    table.flush();

    QueryResult qr = table.queryMetrics(-1, 11, "test", tags);
    Assert.assertEquals(10, qr.getDatapoints().size());

    tags = new TreeMap<>();
    tags.put("host", "otherhost");
    tags.put("dc", "oregon");
    for (int i = 0; i < 10; i += 2) {
      table.writeMetric("test", tags, i, i * 2);
    }

    tags = new TreeMap<>();
    tags.put("dc", "oregon");
    qr = table.queryMetrics(-1, 11, "test", tags);
    Assert.assertEquals(10, qr.getDatapoints().size());
  }
}
