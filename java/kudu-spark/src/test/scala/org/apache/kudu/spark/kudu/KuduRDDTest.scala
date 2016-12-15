package org.apache.kudu.spark.kudu

import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.client.SessionConfiguration.FlushMode
import org.apache.kudu.{Schema, Type}
import org.apache.spark.sql.SQLContext
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

import scala.collection.JavaConverters._
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class KuduRDDTest extends FunSuite with TestContext with Matchers {

  test("test empty batch") {
    val tableName = "test-empty-batch"
    val schema = new Schema(List(new ColumnSchemaBuilder("a", Type.INT32).key(true).build(),
                                 new ColumnSchemaBuilder("b", Type.INT32).build(),
                                 new ColumnSchemaBuilder("c", Type.STRING).build()).asJava)

    val options = new CreateTableOptions
    options.setNumReplicas(1)
    options.setRangePartitionColumns(List("a").asJava)

    kuduClient.createTable(tableName, schema, options)
    val table = kuduClient.openTable(tableName)

    val session = kuduClient.newSession()
    session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)
    // Insert slightly more rows than can fit in a single batch
    for (i <- 0 to 1024 * 1024) {
      val insert = table.newInsert()
      insert.getRow.addInt(0, i)
      insert.getRow.addInt(1, i)
      insert.getRow.addString(2, Random.nextString(1024))
      session.apply(insert)
    }

    session.flush()
    session.countPendingErrors().shouldBe(0)

    val sql = new SQLContext(sc)
    sql.read
       .option("kudu.table", tableName)
       .option("kudu.master", miniCluster.getMasterAddresses)
       .kudu
       .registerTempTable(tableName)

    sql.sql(s"SELECT a, b, c FROM `$tableName` WHERE b = -1").collect().length.shouldBe(1)
  }
}