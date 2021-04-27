package org.pengfei

import org.apache.spark.sql.functions._

object UdfIsEven {
   /* A simple function checks an integer is even or not*/
  def isEven(n: Integer): Boolean = {
    n % 2 == 0
  }

  /* Create an udf based on the isEven function*/
  val isEvenUDF = udf[Boolean, Integer](isEven)

}
