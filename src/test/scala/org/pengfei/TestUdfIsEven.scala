package org.pengfei

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BooleanType, IntegerType, StructField, StructType}
import org.apache.spark.sql.functions.col
import org.scalatest.FunSpec

class TestUdfIsEven extends FunSpec
  with DataFrameComparer
  with SparkSessionTestWrapper{
  import spark.implicits._

  it("appends an is_even column to a Dataframe") {

    val sourceDF = Seq(
      (3),
      (6),
      (8)
    ).toDF("number")

    val actualDF = sourceDF
      .withColumn("is_even", UdfIsEven.isEvenUDF(col("number")))

    val expectedSchema = List(
      StructField("number", IntegerType, false),
      StructField("is_even", BooleanType, false)
    )

    val expectedData = Seq(
      Row(3, false),
      Row(6, true),
      Row(8, true)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )
    actualDF.show()
    expectedDF.show()
    assertSmallDataFrameEquality(actualDF, expectedDF)

  }

  describe(".isEven") {
    it("returns true for even numbers") {
      assert(UdfIsEven.isEven(4) === true)
    }

    it("returns false for odd numbers") {
      assert(UdfIsEven.isEven(3) === false)
    }

    //  it("returns false for null values") {
    //    assert(UdfIsEven.isEven(null) === false)
    //  }
  }
}
