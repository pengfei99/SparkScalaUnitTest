package org.pengfei

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SnakeCaseTransformer {

  /*
  * This function transform a string to lower case and replace space to _*/
  def snakeCase(s: String): String = {
    s.toLowerCase().replace(" ", "_")
  }
/* This function takes a data frame, and transform all column name by using snakeCase function*/
  def snakeCaseColumns(df: DataFrame): DataFrame = {
    /*The fold, foldLeft and foldRight function are very useful in Scala for working with list
    * It has three arguments:
    * - start_value
    * - accumulator
    * - current_element
    * */
    /* In this example, the start value is the data frame. In the first loop, accumulator equals the origin data frame
    * then we rename the first column. In the second loop, accumulator equals a new data frame with first column renamed
    * When the loop is over, all column have been renamed.
    * */
    df.columns.foldLeft(df) { (accumulator, columnName) =>
      accumulator.withColumnRenamed(columnName, snakeCase(columnName))
    }
  }
  }
