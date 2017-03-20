/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kudu.spark.kudu

import java.util.{Collections, Date}

import com.google.common.collect.ImmutableList
import org.apache.avro.SchemaBuilder
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.client.KuduClient.KuduClientBuilder
import org.apache.kudu.client.MiniKuduCluster.MiniKuduClusterBuilder
import org.apache.kudu.client.SessionConfiguration.FlushMode
import org.apache.kudu.client.{CreateTableOptions, KuduClient, KuduTable, MiniKuduCluster}
import org.apache.kudu.{Schema, Type}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert
import org.junit.runner.RunWith
import org.junit.runners.Suite
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.junit.Assert._

@RunWith(classOf[JUnitRunner])
class ScannerTimeoutWorkaround extends FunSuite with BeforeAndAfterAll { self: Suite =>

  var sc: SparkContext = null
  var miniCluster: MiniKuduCluster = null
  var kuduClient: KuduClient = null
  var table: KuduTable = null
  var kuduContext: KuduContext = null

  val appID = new Date().toString + math.floor(math.random * 10E4).toLong.toString

  val conf: SparkConf = new SparkConf().setMaster("local[*]")
                                       .setAppName("test")
                                       .set("spark.ui.enabled", "false")
                                       .set("spark.app.id", appID)

  lazy val schema: Schema = {
    val columns = ImmutableList.of(
      new ColumnSchemaBuilder("key", Type.INT32).key(true).build(),
      new ColumnSchemaBuilder("val", Type.INT32).nullable(false).build())
    new Schema(columns)
  }

  override def beforeAll() {
    miniCluster = new MiniKuduClusterBuilder().numMasters(1)
                                              .numTservers(1)
                                              .addTserverFlag("--scanner-ttl-ms=1000")
                                              .addTserverFlag("--scanner-batch-size-rows=1")
                                              .build()

    sc = new SparkContext(conf)

    kuduClient = new KuduClientBuilder(miniCluster.getMasterAddresses).build()
    assert(miniCluster.waitForTabletServers(1))

    kuduContext = new KuduContext(miniCluster.getMasterAddresses)
  }

  def insertRows(rowCount: Integer): Unit = {
    val kuduSession = kuduClient.newSession()
    for (i <- 0 until rowCount) {
      val insert = table.newInsert
      val row = insert.getRow
      row.addInt(0, i)
      row.addInt(1, i)
      kuduSession.apply(insert)
    }
  }

  override def afterAll() {
    if (kuduClient != null) kuduClient.shutdown()
    if (miniCluster != null) miniCluster.shutdown()
    if (sc != null) sc.stop()
  }

  test("collect rows") {
    val tableOptions = new CreateTableOptions().setRangePartitionColumns(Collections.emptyList())
                                               .setNumReplicas(1)
    val tableName = "scanner-timeout-workaround"
    table = kuduClient.createTable(tableName, schema, tableOptions)


    // Insert 10 rows into the table.
    insertRows(10)

    // Do an extremely slow count operation.  This shouldn't time out, because of the snapshot.
    val sum = kuduContext.kuduRDD(sc, tableName, List("key"))
                         .map(_.getInt(0))
                         .fold(0)({ (acc, n) =>
                           Thread.sleep(1000)
                           acc + n
                         })

    assertEquals(sum, 45)
  }
}
