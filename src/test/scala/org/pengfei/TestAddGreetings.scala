package org.pengfei

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.FunSpec

/*This class tests the function of Object AddGreetings. Because it has only one function add().*/
class TestAddGreetings extends FunSpec with DataFrameComparer with SparkSessionTestWrapper {

  import spark.implicits._
  it("appends a greeting column to a Dataframe") {

    val sourceDF = Seq(
      ("titi"),
      ("toto")
    ).toDF("name")

    val actualDF = sourceDF.transform(AddGreetings.add())

    /*A schema is a list of structField, a structField contains three elements:
    * - col name
    * - type
    * - nullable: if true, column can be null. if false, column cannot contain null value
    * */

    val expectedSchema = List(
      StructField("name", StringType, true),
      StructField("greeting", StringType, false)
    )

    val expectedData = Seq(
      Row("titi", "hello world"),
      Row("toto", "hello world")
    )

    /* here we don't use the helper method toDF(). Because we don't want to have null value in greeting column*/
    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    //This function is from com.github.mrpowers.spark.fast.tests. It compares the equality of two data frame.
    assertSmallDataFrameEquality(actualDF, expectedDF)

  }


}
