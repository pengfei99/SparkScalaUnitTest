package org.pengfei

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit


object AddGreetings {

  def add()(df: DataFrame):DataFrame={
    df.withColumn("greeting", lit("hello world"))
  }

}
