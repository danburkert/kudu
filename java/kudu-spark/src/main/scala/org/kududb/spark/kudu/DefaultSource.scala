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

import java.sql.Timestamp

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.kududb.Type
import org.kududb.annotations.InterfaceStability
import org.kududb.client._
import org.kududb.client.KuduPredicate.ComparisonOp
import org.kududb.client.SessionConfiguration.FlushMode
import org.apache.spark.sql.SaveMode._

import scala.collection.JavaConverters._

/**
  * DefaultSource for integration with Spark's dataframe datasources.
  * This class will produce a relationProvider based on input given to it from spark.
  */
@InterfaceStability.Unstable
class DefaultSource extends RelationProvider with CreatableRelationProvider {

  val TABLE_KEY = "kudu.table"
  val KUDU_MASTER = "kudu.master"

  /**
    * Construct a BaseRelation using the provided context and parameters.
    *
    * @param sqlContext SparkSQL context
    * @param parameters parameters given to us from SparkSQL
    * @return           a BaseRelation Object
    */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]):
  BaseRelation = {
    val tableName = parameters.getOrElse(TABLE_KEY,
      throw new IllegalArgumentException(s"Kudu table name must be specified in create options using key '$TABLE_KEY'"))
    val kuduMaster = parameters.getOrElse(KUDU_MASTER, "localhost")

    new KuduRelation(tableName, kuduMaster)(sqlContext)
  }

  /**
    * Creates a relation and inserts data to specified table.
    *
    * @param sqlContext
    * @param mode Append will not overwrite existing data, Overwrite will perform update, but will not insert data, use upsert on KuduContext if you require both
    * @param parameters Nessisary parameters for kudu.table and kudu.master
    * @param data Dataframe to save into kudu
    * @return returns populated base relation
    */
  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val tableName = parameters.getOrElse(TABLE_KEY,
      throw new IllegalArgumentException(s"Kudu table name must be specified in create options using key '$TABLE_KEY'"))

    val kuduMaster = parameters.getOrElse(KUDU_MASTER, "localhost")

    val kuduRelation = new KuduRelation(tableName, kuduMaster)(sqlContext)
    mode match {
      case Append => kuduRelation.insert(data, overwrite = false)
      case Overwrite => kuduRelation.insert(data, overwrite = true)
      case ErrorIfExists =>
          throw new UnsupportedOperationException(
            "ErrorIfExists is currently not supported")
      case Ignore => kuduRelation.insert(data, overwrite = false)
    }

    kuduRelation
  }
}

/**
  * Implementation of Spark BaseRelation.
  *
  * @param tableName Kudu table that we plan to read from
  * @param kuduMaster Kudu master addresses
  * @param sqlContext SparkSQL context
  */
@InterfaceStability.Unstable
class KuduRelation(private val tableName: String,
                   private val kuduMaster: String)(
                   val sqlContext: SQLContext)
extends BaseRelation
with PrunedFilteredScan
with InsertableRelation {

  import KuduRelation._

  private val context: KuduContext = new KuduContext(kuduMaster)
  private val table: KuduTable = context.syncClient.openTable(tableName)

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] =
    filters.filterNot(supportsFilter)

  /**
    * Generates a SparkSQL schema object so SparkSQL knows what is being
    * provided by this BaseRelation.
    *
    * @return schema generated from the Kudu table's schema
    */
  override def schema: StructType = {
    val fields: Array[StructField] =
      table.getSchema.getColumns.asScala.map { columnSchema =>
        val sparkType = kuduTypeToSparkType(columnSchema.getType)
        new StructField(columnSchema.getName, sparkType, columnSchema.isNullable)
      }.toArray

    new StructType(fields)
  }

  /**
    * Build the RDD to scan rows.
    *
    * @param requiredColumns columns that are being requested by the requesting query
    * @param filters         filters that are being applied by the requesting query
    * @return RDD will all the results from Kudu
    */
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val predicates = filters.flatMap(filterToPredicate)
    new KuduRDD(kuduMaster, 1024 * 1024 * 20, requiredColumns, predicates,
      table, context, sqlContext.sparkContext)
  }

  /**
    * Converts a Spark [[Filter]] to a Kudu [[KuduPredicate]].
    *
    * @param filter the filter to convert
    * @return the converted filter
    */
  private def filterToPredicate(filter : Filter) : Array[KuduPredicate] = {
    filter match {
      case EqualTo(column, value) =>
        Array(comparisonPredicate(column, ComparisonOp.EQUAL, value))
      case GreaterThan(column, value) =>
        Array(comparisonPredicate(column, ComparisonOp.GREATER, value))
      case GreaterThanOrEqual(column, value) =>
        Array(comparisonPredicate(column, ComparisonOp.GREATER_EQUAL, value))
      case LessThan(column, value) =>
        Array(comparisonPredicate(column, ComparisonOp.LESS, value))
      case LessThanOrEqual(column, value) =>
        Array(comparisonPredicate(column, ComparisonOp.LESS_EQUAL, value))
      case And(left, right) => filterToPredicate(left) ++ filterToPredicate(right)
      case _ => Array()
    }
  }

  /**
    * Creates a new comparison predicate for the column, comparison operator, and comparison value.
    *
    * @param column the column name
    * @param operator the comparison operator
    * @param value the comparison value
    * @return the comparison predicate
    */
  private def comparisonPredicate(column: String,
                                  operator: ComparisonOp,
                                  value: Any): KuduPredicate = {
    val columnSchema = table.getSchema.getColumn(column)
    value match {
      case value: Boolean => KuduPredicate.newComparisonPredicate(columnSchema, operator, value)
      case value: Byte => KuduPredicate.newComparisonPredicate(columnSchema, operator, value)
      case value: Short => KuduPredicate.newComparisonPredicate(columnSchema, operator, value)
      case value: Int => KuduPredicate.newComparisonPredicate(columnSchema, operator, value)
      case value: Long => KuduPredicate.newComparisonPredicate(columnSchema, operator, value)
      case value: Timestamp => KuduPredicate.newComparisonPredicate(columnSchema, operator, timestampToMicros(value))
      case value: Float => KuduPredicate.newComparisonPredicate(columnSchema, operator, value)
      case value: Double => KuduPredicate.newComparisonPredicate(columnSchema, operator, value)
      case value: String => KuduPredicate.newComparisonPredicate(columnSchema, operator, value)
      case value: Array[Byte] => KuduPredicate.newComparisonPredicate(columnSchema, operator, value)
    }
  }

  /**
    * Inserts data into an existing kudu table.
    * @param data Dataframe to be inserted into kudu
    * @param overwrite If True it will update existing records, but will not perform inserts.
    */
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    context.writeRows(data, tableName, overwrite)
  }
}

private[spark] object KuduRelation {
  /**
    * Converts a Kudu [[Type]] to a Spark SQL [[DataType]].
    *
    * @param t the Kudu type
    * @return the corresponding Spark SQL type
    */
  private def kuduTypeToSparkType(t: Type): DataType = t match {
    case Type.BOOL => BooleanType
    case Type.INT8 => ByteType
    case Type.INT16 => ShortType
    case Type.INT32 => IntegerType
    case Type.INT64 => LongType
    case Type.TIMESTAMP => TimestampType
    case Type.FLOAT => FloatType
    case Type.DOUBLE => DoubleType
    case Type.STRING => StringType
    case Type.BINARY => BinaryType
  }

  /**
    * Returns `true` if the filter is able to be pushed down to Kudu.
    *
    * @param filter the filter to test
    */
  private def supportsFilter(filter: Filter): Boolean = filter match {
    case EqualTo(_, _)
       | GreaterThan(_, _)
       | GreaterThanOrEqual(_, _)
       | LessThan(_, _)
       | LessThanOrEqual(_, _) => true
    case And(left, right) => supportsFilter(left) && supportsFilter(right)
    case _ => false
  }

  /**
    * Converts a [[Timestamp]] to microseconds since the Unix epoch (1970-01-01T00:00:00Z).
    *
    * @param timestamp the timestamp to convert to microseconds
    * @return the microseconds since the Unix epoch
    */
  def timestampToMicros(timestamp: Timestamp): Long = {
    // Number of whole milliseconds since the Unix epoch, in microseconds.
    val millis = timestamp.getTime * 1000
    // Sub millisecond time since the Unix epoch, in microseconds.
    val micros = (timestamp.getNanos % 1000000) / 1000
    if (micros >= 0) {
      millis + micros
    } else {
      millis + 1000000 + micros
    }
  }

  /**
    * Converts a microsecond offset from the Unix epoch (1970-01-01T00:00:00Z) to a [[Timestamp]].
    *
    * @param micros the offset in microseconds since the Unix epoch
    * @return the corresponding timestamp
    */
  def microsToTimestamp(micros: Long): Timestamp = {
    var millis = micros / 1000
    var nanos = (micros % 1000000) * 1000
    if (nanos < 0) {
      millis -= 1
      nanos += 1000000000
    }

    val timestamp = new Timestamp(millis)
    timestamp.setNanos(nanos.asInstanceOf[Int])
    timestamp
  }
}
