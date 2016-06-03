// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package org.kududb.client;

import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.stumbleupon.async.Deferred;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;
import org.kududb.Common;
import org.kududb.consensus.Metadata;
import org.kududb.master.Master;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class TestAsyncKuduClient extends BaseKuduTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestAsyncKuduClient.class);

  private static final String TABLE_NAME =
      TestAsyncKuduClient.class.getName() + "-" + System.currentTimeMillis();
  private static KuduTable table;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    BaseKuduTest.setUpBeforeClass();
    // Set to 1 for testDisconnect to always test disconnecting the right server.
    CreateTableOptions options = getBasicCreateTableOptions().setNumReplicas(1);
    table = createTable(TABLE_NAME, basicSchema, options);
  }

  @Test(timeout = 100000)
  public void testDisconnect() throws Exception {
    // Test that we can reconnect to a TS after a disconnection.
    // 1. Warm up the cache.
    assertEquals(0, countRowsInScan(client.newScannerBuilder(table).build()));

    // 2. Disconnect the client.
    disconnectAndWait();

    // 3. Count again, it will trigger a re-connection and we should not hang or fail to scan.
    assertEquals(0, countRowsInScan(client.newScannerBuilder(table).build()));

    // Test that we can reconnect to a TS while scanning.
    // 1. Insert enough rows to have to call next() multiple times.
    KuduSession session = syncClient.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
    int rowCount = 200;
    for (int i = 0; i < rowCount; i++) {
      session.apply(createBasicSchemaInsert(table, i));
    }
    session.flush();

    // 2. Start a scanner with a small max num bytes.
    AsyncKuduScanner scanner = client.newScannerBuilder(table)
        .batchSizeBytes(1)
        .build();
    Deferred<RowResultIterator> rri = scanner.nextRows();
    // 3. Register the number of rows we get back. We have no control over how many rows are
    // returned. When this test was written we were getting 100 rows back.
    int numRows = rri.join(DEFAULT_SLEEP).getNumRows();
    assertNotEquals("The TS sent all the rows back, we can't properly test disconnection",
        rowCount, numRows);

    // 4. Disconnect the client.
    disconnectAndWait();

    // 5. Make sure that we can continue scanning and that we get the remaining rows back.
    assertEquals(rowCount - numRows, countRowsInScan(scanner));
  }

  private void disconnectAndWait() throws InterruptedException {
    for (TabletClient tabletClient : client.getTabletClients()) {
      tabletClient.disconnect();
    }
    Stopwatch sw = Stopwatch.createStarted();
    while (sw.elapsed(TimeUnit.MILLISECONDS) < DEFAULT_SLEEP) {
      if (!client.getTabletClients().isEmpty()) {
        Thread.sleep(50);
      } else {
        break;
      }
    }
    assertTrue(client.getTabletClients().isEmpty());
  }

  @Test
  public void testBadHostnames() throws Exception {
    String badHostname = "some-unknown-host-hopefully";

    // Test that a bad hostname for the master makes us error out quickly.
    AsyncKuduClient invalidClient = new AsyncKuduClient.AsyncKuduClientBuilder(badHostname).build();
    try {
      invalidClient.listTabletServers().join(1000);
      fail("This should have failed quickly");
    } catch (Exception ex) {
      assertTrue(ex instanceof NonRecoverableException);
      assertTrue(ex.getMessage().contains(badHostname));
    }

    List<Master.TabletLocationsPB> tabletLocations = new ArrayList<>();

    // Builder three bad locations.
    Master.TabletLocationsPB.Builder tabletPb = Master.TabletLocationsPB.newBuilder();
    for (int i = 0; i < 3; i++) {
      Common.PartitionPB.Builder partition = Common.PartitionPB.newBuilder();
      partition.setPartitionKeyStart(ByteString.copyFrom("a" + i, Charsets.UTF_8.name()));
      partition.setPartitionKeyEnd(ByteString.copyFrom("b" + i, Charsets.UTF_8.name()));
      tabletPb.setPartition(partition);
      tabletPb.setTabletId(ByteString.copyFromUtf8("some id " + i));
      Master.TSInfoPB.Builder tsInfoBuilder = Master.TSInfoPB.newBuilder();
      Common.HostPortPB.Builder hostBuilder = Common.HostPortPB.newBuilder();
      hostBuilder.setHost(badHostname + i);
      hostBuilder.setPort(i);
      tsInfoBuilder.addRpcAddresses(hostBuilder);
      tsInfoBuilder.setPermanentUuid(ByteString.copyFromUtf8("some uuid"));
      Master.TabletLocationsPB.ReplicaPB.Builder replicaBuilder =
          Master.TabletLocationsPB.ReplicaPB.newBuilder();
      replicaBuilder.setTsInfo(tsInfoBuilder);
      replicaBuilder.setRole(Metadata.RaftPeerPB.Role.FOLLOWER);
      tabletPb.addReplicas(replicaBuilder);
      tabletLocations.add(tabletPb.build());
    }

    // Test that a tablet full of unreachable replicas won't make us retry.
    try {
      KuduTable badTable = new KuduTable(client, "Invalid table name",
                                         "Invalid table ID", null, null);
      client.discoverTablets(badTable, tabletLocations);
      fail("This should have failed quickly");
    } catch (Exception ex) {
      assertTrue(ex instanceof NonRecoverableException);
      assertTrue(ex.getMessage().contains(badHostname));
    }
  }

  @Test(timeout = 100000)
  public void testLocateTable() throws Exception {
    String tableName = TestAsyncKuduClient.class.getName() + "-testLocateTable";
    CreateTableOptions tableOptions = new CreateTableOptions();
    tableOptions.setRangePartitionColumns(ImmutableList.of("key"));

    PartialRow split1 = basicSchema.newPartialRow();
    split1.addInt("key", 100);
    tableOptions.addSplitRow(split1);

    PartialRow split2 = basicSchema.newPartialRow();
    split2.addInt("key", 200);
    tableOptions.addSplitRow(split2);

    PartialRow split3 = basicSchema.newPartialRow();
    split3.addInt("key", 300);
    tableOptions.addSplitRow(split3);

    client.createTable(tableName, basicSchema, tableOptions).join();
    KuduTable table = client.openTable(tableName).join();

    byte[] emptyKey = new byte[0];
    byte[] key1 = new byte[] {(byte) 0x80, 0x00, 0x00, 0x64 };
    byte[] key2 = new byte[] {(byte) 0x80, 0x00, 0x00, (byte) 0xC8};
    byte[] key3 = new byte[] {(byte) 0x80, 0x00, 0x01, (byte) 0x2C};


    { // all tablets
      List<LocatedTablet> tablets = client.syncLocateTable(table.getTableId(), emptyKey,
                                                           null, 100000);
      assertEquals(4, tablets.size());
      assertArrayEquals(emptyKey, tablets.get(0).getPartition().getPartitionKeyStart());
      assertArrayEquals(key1, tablets.get(0).getPartition().getPartitionKeyEnd());
      assertArrayEquals(key1, tablets.get(1).getPartition().getPartitionKeyStart());
      assertArrayEquals(key2, tablets.get(1).getPartition().getPartitionKeyEnd());
      assertArrayEquals(key2, tablets.get(2).getPartition().getPartitionKeyStart());
      assertArrayEquals(key3, tablets.get(2).getPartition().getPartitionKeyEnd());
      assertArrayEquals(key3, tablets.get(3).getPartition().getPartitionKeyStart());
      assertArrayEquals(emptyKey, tablets.get(3).getPartition().getPartitionKeyEnd());
    }

    { // key < 50
      List<LocatedTablet> tablets = client.syncLocateTable(table.getTableId(),
                                                           null,
                                                           new byte[] { (byte) 0x80, 0x00, 0x00, 0x32 },
                                                           100000);
      assertEquals(1, tablets.size());
      assertArrayEquals(emptyKey, tablets.get(0).getPartition().getPartitionKeyStart());
      assertArrayEquals(key1, tablets.get(0).getPartition().getPartitionKeyEnd());
    }

    { // key >= 300
      List<LocatedTablet> tablets = client.syncLocateTable(table.getTableId(), key3, null, 100000);
      assertEquals(1, tablets.size());
      assertArrayEquals(key3, tablets.get(0).getPartition().getPartitionKeyStart());
      assertArrayEquals(emptyKey, tablets.get(0).getPartition().getPartitionKeyEnd());
    }

    { // key >= 299
      List<LocatedTablet> tablets = client.syncLocateTable(table.getTableId(),
                                                           new byte[] {(byte) 0x80, 0x00, 0x01, 0x2B },
                                                           null, 100000);
      assertEquals(2, tablets.size());
      assertArrayEquals(key2, tablets.get(0).getPartition().getPartitionKeyStart());
      assertArrayEquals(key3, tablets.get(0).getPartition().getPartitionKeyEnd());
      assertArrayEquals(key3, tablets.get(1).getPartition().getPartitionKeyStart());
      assertArrayEquals(emptyKey, tablets.get(1).getPartition().getPartitionKeyEnd());
    }

    { // key >= 150 && key < 250
      List<LocatedTablet> tablets = client.syncLocateTable(table.getTableId(),
                                                           new byte[] {(byte) 0x80, 0x00, 0x00, (byte) 0x96 },
                                                           new byte[] {(byte) 0x80, 0x00, 0x00, (byte) 0xFA },
                                                           100000);
      assertEquals(2, tablets.size());
      assertArrayEquals(key1, tablets.get(0).getPartition().getPartitionKeyStart());
      assertArrayEquals(key2, tablets.get(0).getPartition().getPartitionKeyEnd());
      assertArrayEquals(key2, tablets.get(1).getPartition().getPartitionKeyStart());
      assertArrayEquals(key3, tablets.get(1).getPartition().getPartitionKeyEnd());
    }
  }

  @Test(timeout = 100000)
  public void testLocateNonExistentTable() throws Exception {
    try {
      client.syncLocateTable("bogus-table-id", null, null, 100000);
    } catch (MasterErrorException e) {
      assertTrue(e.getMessage().contains("table does not exist"));
      return;
    }
    assertTrue("unreachable", false);
  }
}
