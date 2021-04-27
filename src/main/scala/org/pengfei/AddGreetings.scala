package org.pengfei

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit


object AddGreetings {
/* This function simply add a new column "greeting" to the source data frame. All the cell contains the value
* "hello world"*/
  def add()(df: DataFrame):DataFrame={
    df.withColumn("greeting", lit("hello world"))
  }

}
