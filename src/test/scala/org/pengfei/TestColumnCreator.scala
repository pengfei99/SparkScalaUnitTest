package org.pengfei

import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.scalatest.FunSpec


class TestColumnCreator
  extends FunSpec
    with ColumnComparer
    with DataFrameComparer
    with SparkSessionTestWrapper {

  import spark.implicits._


  describe(".createColumnWithPower") {

    /** Normal column equality test */
    it("create a new column which is the Power 2 of the source column") {

      val sourceDF = Seq(
        (1, 1),
        (2, 4),
        (3, 9),
        (4, 16),
      ).toDF("source", "expected")

      val df = ColumnCreator.createColumnWithPower(sourceDF,"source")

      print("Generated df:")
      df.show()
      assertColumnEquality(df, "power_2", "expected")

    }


    /** Approximate column equality test, note columns must have double, or float type */
    it("if the column is of type float or double, we can use a precision to test approximate equality ") {

      val dfSchema = List(
        StructField("source",DoubleType,false),
        StructField("expected",DoubleType,false)
      )

      val dfData = Seq(
        Row(1.0, 1.01),
        Row(2.0, 4.06),
        Row(3.0, 9.001),
        Row(4.0, 16.001)
      )

      val sourceDF1 = spark.createDataFrame(
        spark.sparkContext.parallelize(dfData),
        StructType(dfSchema)
      )

      val df1 = ColumnCreator.createColumnWithPower(sourceDF1,"source")

      print("Generated df:")
      df1.show()
      print("Source df:")
        sourceDF1.show()
      assertDoubleTypeColumnEquality(df1,"power_2","expected",0.1)
     // assertColumnEquality(df, "power_2", "expected")

    }

    /** Approximate data frame equality test, column must be float or double */
    it("Test with Approximate DataFrame Equality with float, double precision") {

      /*note the data frame generated by toDF, the nullable property of column is false by default*/
      val sourceDF2 = Seq(
        (1.0),
        (2.0),
        (3.0),
        (4.0)
      ).toDF("source")

      val generatedDF2 = ColumnCreator.createColumnWithPower(sourceDF2,"source")



      val expectedSchema2 = List(
        StructField("source",DoubleType,false),
        StructField("power_2",DoubleType,true)
      )

      val expectedData2 = Seq(
        Row(1.0, 1.01),
        Row(2.0, 4.06),
        Row(3.0, 9.001),
        Row(4.0, 16.001)
      )

      val expectedDF2 = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData2),
        StructType(expectedSchema2)
      )

      print("Source df:")
      sourceDF2.show()
      sourceDF2.printSchema()
      print("Generated df:")
      generatedDF2.show()
      generatedDF2.printSchema()
      print("Expected df:")
      expectedDF2.show()
      expectedDF2.printSchema()

      /* The implementation of this method is not complete. So if not equal, if won't show a diff map, but throw a
      * exception
      * */
      assertApproximateDataFrameEquality(generatedDF2, expectedDF2, 0.1,ignoreNullable = true)

    }

  }

}
