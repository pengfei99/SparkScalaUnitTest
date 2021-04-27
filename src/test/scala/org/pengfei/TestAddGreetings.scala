package org.pengfei

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.FunSpec

class TestAddGreetings extends FunSpec with DataFrameComparer with SparkSessionTestWrapper {

  import spark.implicits._
  it("appends a greeting column to a Dataframe") {

    val sourceDF = Seq(
      ("titi"),
      ("toto")
    ).toDF("name")

    val actualDF = sourceDF.transform(AddGreetings.add())

    val expectedSchema = List(
      StructField("name", StringType, true),
      StructField("greeting", StringType, false)
    )

    val expectedData = Seq(
      Row("titi", "hello world"),
      Row("toto", "hello world")
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    assertSmallDataFrameEquality(actualDF, expectedDF)

  }


}
