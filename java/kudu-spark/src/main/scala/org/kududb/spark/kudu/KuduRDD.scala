package org.kududb.spark.kudu

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.kududb.client._
import org.kududb.{ColumnSchema, Schema, Type, client}

import scala.collection.JavaConverters._
import scala.collection.mutable

class KuduRDD(@transient sc: SparkContext,
              val kuduMaster: String,
              val tableName: String,
              @transient batchSize: Integer,
              @transient projectedCols: Array[String],
              @transient predicates: Array[client.KuduPredicate]) extends RDD[Row](sc, Nil) {

  override protected def getPartitions: Array[Partition] = {
    val client: KuduClient = new KuduContext(kuduMaster).syncClient

    val table: KuduTable = client.openTable(tableName)

    val builder = client.newScanTokenBuilder(table)
                        .batchSizeBytes(batchSize)
                        .setProjectedColumnNames(projectedCols.toSeq.asJava)
                        .cacheBlocks(true)

    for (predicate <- predicates) {
      builder.addPredicate(predicate)
    }
    val tokens = builder.build().asScala
    tokens.zipWithIndex.map {
      case (token, index) =>
        new KuduPartition(index, token.serialize(),
                          token.getTablet.getReplicas.asScala.map(_.getRpcHost).toArray)
    }.toArray
  }

  override def compute(part: Partition, taskContext: TaskContext): Iterator[Row] = {
    val client: KuduClient = new KuduContext(kuduMaster).syncClient
    val partition: KuduPartition = part.asInstanceOf[KuduPartition]
    val scanner = KuduScanToken.deserializeIntoScanner(partition.scanToken, client)
    new RowResultIteratorScala(scanner)
  }

  def buildKuduSchemaColumnMap(kuduSchema: Schema): mutable.HashMap[String, ColumnSchema] = {

    var kuduSchemaColumnMap = new mutable.HashMap[String, ColumnSchema]()

    val columnIt = kuduSchema.getColumns.iterator()
    while (columnIt.hasNext) {
      val c = columnIt.next()
      kuduSchemaColumnMap.+=((c.getName, c))
    }
    kuduSchemaColumnMap
  }

  override def getPreferredLocations(partition: Partition): Seq[String] = {
    partition.asInstanceOf[KuduPartition].locations
  }
}

private[spark] class KuduPartition(val index: Int,
                                   val scanToken: Array[Byte],
                                   val locations : Array[String]) extends Partition {}

private[spark] class RowResultIteratorScala(scanner: KuduScanner) extends Iterator[Row] {

  var currentIterator: RowResultIterator = null

  override def hasNext: Boolean = {
    if ((currentIterator != null && !currentIterator.hasNext && scanner.hasMoreRows) ||
      (scanner.hasMoreRows && currentIterator == null)) {
      currentIterator = scanner.nextRows()
    }
    currentIterator.hasNext
  }

  override def next(): Row = new KuduRow(currentIterator.next())
}

/**
  * A Spark SQL [[Row]] which wraps a Kudu [[RowResult]].
  * @param rowResult the wrapped row result
  */
class KuduRow(private val rowResult: RowResult) extends Row {
  override def length: Int = rowResult.getColumnProjection.getColumnCount

  override def get(i: Int): Any = {
    if (rowResult.isNull(i)) null
    else rowResult.getColumnType(i) match {
      case Type.BOOL => rowResult.getBoolean(i)
      case Type.INT8 => rowResult.getByte(i)
      case Type.INT16 => rowResult.getShort(i)
      case Type.INT32 => rowResult.getInt(i)
      case Type.INT64 => rowResult.getLong(i)
      case Type.TIMESTAMP => KuduRelation.microsToTimestamp(rowResult.getLong(i))
      case Type.FLOAT => rowResult.getFloat(i)
      case Type.DOUBLE => rowResult.getDouble(i)
      case Type.STRING => rowResult.getString(i)
      case Type.BINARY => rowResult.getBinary(i)
    }
  }

  override def copy(): Row = Row.fromSeq(Range(0, length).map(get))

  override def toString(): String = rowResult.toString
}
