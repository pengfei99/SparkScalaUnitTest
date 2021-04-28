package org.pengfei

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col,udf}

object ColumnCreator {
  def power(n: Int): Double = {
    math.pow(n,2)
  }

  val powerUDF = udf[Double, Int](power)

  def createColumnWithPower(df: DataFrame, columnName: String) = {
    df.withColumn("power_2", powerUDF(col(columnName)))
  }
}
