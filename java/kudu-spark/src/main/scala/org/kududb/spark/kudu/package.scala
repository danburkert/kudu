
package org.kududb.spark

import org.apache.spark.sql.{DataFrame, DataFrameReader}

package object kudu {

  /**
   * Adds a method, `kudu`, to DataFrameReader that allows you to read Kudu tables using
   * the DataFrameReader.
   */
  implicit class KuduDataFrameReader(reader: DataFrameReader) {
    def kudu: DataFrame = reader.format("org.kududb.spark.kudu").load
  }
}
