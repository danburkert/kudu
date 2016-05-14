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

package org.kududb.spark.kudu

import java.util

import org.apache.hadoop.util.ShutdownHookManager
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.kududb.annotations.InterfaceStability
import org.kududb.client.SessionConfiguration.FlushMode
import org.kududb.client._
import org.kududb.{ColumnSchema, Schema, Type}


/**
  * KuduContext is a serializable container for Kudu client connections.
  *
  * If a Kudu client connection is needed as part of a Spark application, a
  * [[KuduContext]] should used as a broadcast variable in the job in order to
  * share connections among the tasks in a JVM.
  */
@InterfaceStability.Unstable
class KuduContext(kuduMaster: String) extends Serializable {

  /**
    * Set to
    * [[org.apache.spark.util.ShutdownHookManager.DEFAULT_SHUTDOWN_PRIORITY]].
    * The client instances are closed through the JVM shutdown hook
    * mechanism in order to make sure that any unflushed writes are cleaned up
    * properly. Spark has no shutdown notifications.
    */
  private val ShutdownHookPriority = 100

  @transient lazy val syncClient = {
    val syncClient = new KuduClient.KuduClientBuilder(kuduMaster).build()
    ShutdownHookManager.get().addShutdownHook(new Runnable {
      override def run() = syncClient.close()
    }, ShutdownHookPriority)
    syncClient
  }

  @transient lazy val asyncClient = {
    val asyncClient = new AsyncKuduClient.AsyncKuduClientBuilder(kuduMaster).build()
    ShutdownHookManager.get().addShutdownHook(
      new Runnable {
        override def run() = asyncClient.close()
      }, ShutdownHookPriority)
    asyncClient
  }

  /**
    * Create an RDD from a Kudu table.
    *
    * @param tableName          table to read from
    * @param columnProjection   list of columns to read. Not specifying this at all
    *                           (i.e. setting to null) or setting to the special
    *                           string '*' means to project all columns.
    * @return a new RDD that maps over the given table for the selected columns
    */
  def kuduRDD(sc: SparkContext,
              tableName: String,
              columnProjection: Seq[String] = Nil): RDD[Row] = {
    new KuduRDD(kuduMaster, 1024*1024*20, columnProjection.toArray, Array(),
                syncClient.openTable(tableName), this, sc)
  }

  /**
    * Check if kudu table already exists
    * @param tableName tablename to check
    * @return true if table exists, false if table does not exist
    */
  def tableExists(tableName: String): Boolean = syncClient.tableExists(tableName)

  /**
    * Delete kudu table
    * @param tableName tablename to delete
    * @return DeleteTableResponse
    */
  def deleteTable(tableName: String): DeleteTableResponse = syncClient.deleteTable(tableName)

  /**
    * Creates a kudu table for the given schema. Partitioning can be specified through options.
    * @param tableName table to create
    * @param schema struct schema of table
    * @param keys primary keys of the table
    * @param options replication and partitioning options for the table
    */
  def createTable(tableName: String,
                  schema: StructType,
                  keys: Seq[String],
                  options: CreateTableOptions): KuduTable = {
    val kuduCols = new util.ArrayList[ColumnSchema]()
    // add the key columns first, in the order specified
    for (key <- keys) {
      val f = schema.fields(schema.fieldIndex(key))
      kuduCols.add(new ColumnSchema.ColumnSchemaBuilder(f.name, kuduType(f.dataType)).key(true).build())
    }
    // now add the non-key columns
    for (f <- schema.fields.filter(field=> !keys.contains(field.name))) {
      kuduCols.add(new ColumnSchema.ColumnSchemaBuilder(f.name, kuduType(f.dataType)).nullable(f.nullable).key(false).build())
    }

    syncClient.createTable(tableName, new Schema(kuduCols), options)
  }

  /** Map Spark SQL type to Kudu type */
  def kuduType(dt: DataType) : Type = dt match {
    case DataTypes.BinaryType => Type.BINARY
    case DataTypes.BooleanType => Type.BOOL
    case DataTypes.StringType => Type.STRING
    case DataTypes.TimestampType => Type.TIMESTAMP
    case DataTypes.ByteType => Type.INT8
    case DataTypes.ShortType => Type.INT16
    case DataTypes.IntegerType => Type.INT32
    case DataTypes.LongType => Type.INT64
    case DataTypes.FloatType => Type.FLOAT
    case DataTypes.DoubleType => Type.DOUBLE
    case _ => throw new RuntimeException(s"No support for Spark SQL type $dt")
  }

  /**
    * Inserts or updates rows in kudu from a [[DataFrame]].
    * @param data Dataframe to insert/update
    * @param tableName Table to perform insertion on
    * @param overwrite true=update, false=insert
    */
  def writeRows(data: DataFrame, tableName: String, overwrite: Boolean) {
    val schema = data.schema
    data.foreachPartition(iterator => {
      val pendingErrors = writeRows(iterator, schema, tableName, overwrite)
      val errorCount = pendingErrors.getRowErrors.length
      if (errorCount > 0) {
        val errors = pendingErrors.getRowErrors.take(5).map(_.getErrorStatus).mkString
        throw new RuntimeException(
          s"failed to write $errorCount rows from DataFrame to Kudu; sample errors: $errors")
      }
    })
  }

  /**
    * Saves partitions of a dataframe into Kudu.
    *
    * @param rows rows to insert or update
    * @param tableName table to insert or update on
    */
  def writeRows(rows: Iterator[Row],
                schema: StructType,
                tableName: String,
                performAsUpdate : Boolean = false): RowErrorsAndOverflowStatus = {
    val table: KuduTable = syncClient.openTable(tableName)
    val kuduSchema = table.getSchema

    val indices: Array[(Int, Int)] = schema.fields.zipWithIndex.map({ case (field, sparkIdx) =>
      sparkIdx -> table.getSchema.getColumnIndex(field.name)
    })

    val session: KuduSession = syncClient.newSession
    session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)
    session.setIgnoreAllDuplicateRows(true)
    try {
      for (row <- rows) {
        val operation = if (performAsUpdate) { table.newUpdate() } else { table.newInsert() }
        for ((sparkIdx, kuduIdx) <- indices) {
          if (row.isNullAt(sparkIdx)) {
            operation.getRow.setNull(kuduIdx)
          } else schema.fields(sparkIdx).dataType match {
            case DataTypes.StringType => operation.getRow.addString(kuduIdx, row.getString(sparkIdx))
            case DataTypes.BinaryType => operation.getRow.addBinary(kuduIdx, row.getAs[Array[Byte]](sparkIdx))
            case DataTypes.BooleanType => operation.getRow.addBoolean(kuduIdx, row.getBoolean(sparkIdx))
            case DataTypes.ByteType => operation.getRow.addByte(kuduIdx, row.getByte(sparkIdx))
            case DataTypes.ShortType => operation.getRow.addShort(kuduIdx, row.getShort(sparkIdx))
            case DataTypes.IntegerType => operation.getRow.addInt(kuduIdx, row.getInt(sparkIdx))
            case DataTypes.LongType => operation.getRow.addLong(kuduIdx, row.getLong(sparkIdx))
            case DataTypes.FloatType => operation.getRow.addFloat(kuduIdx, row.getFloat(sparkIdx))
            case DataTypes.DoubleType => operation.getRow.addDouble(kuduIdx, row.getDouble(sparkIdx))
            case DataTypes.TimestampType => operation.getRow.addLong(kuduIdx, KuduRelation.timestampToMicros(row.getTimestamp(sparkIdx)))
            case _ => throw new IllegalArgumentException(s"No support for Spark SQL type $row")
          }
        }
        session.apply(operation)
      }
    } finally {
      session.close()
    }
    session.getPendingErrors
  }
}