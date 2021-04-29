package org.pengfei

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.FunSpec

/*This class tests the function of Object AddGreetings. Because it has only one function add().*/
class TestAddGreetings extends FunSpec with DataFrameComparer with SparkSessionTestWrapper {

  import spark.implicits._

  describe(".add") {
    it("appends a greeting column to a Dataframe") {

      val sourceDF = Seq(
        ("titi"),
        ("toto"),
        ("tata"),
        ("foo"),
        ("bar")
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
        Row("toto", "hello world"),
        Row("tata", "hello world"),
        Row("foo", "hello world"),
        Row("bar", "hello world")
      )

      /* here we don't use the helper method toDF(). Because we don't want to have null value in greeting column*/
      val expectedDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      //This function is from com.github.mrpowers.spark.fast.tests. It compares the equality of two data frame.
      //Before compare the values of two data frames, it compares first the schema. If schema is not equal. return false
      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

    /************************** Unordered DataFrame equality comparisons ****************************************/
    /** Some times even though two data frame are equal, but the row order are different which makes the assert function
     * returns false. To avoid this, we can add option orderedComparison = false in the assert function. Check below
     * example*/
    it("test the equality of two Data frame by ignoring row order") {

      val sourceDF1 = Seq(
        ("titi"),
        ("toto"),
        ("tata"),
        ("foo"),
        ("bar")
      ).toDF("name")

      val actualDF1 = sourceDF1.transform(AddGreetings.add())

      /*A schema is a list of structField, a structField contains three elements:
    * - col name
    * - type
    * - nullable: if true, column can be null. if false, column cannot contain null value
    * */

      val expectedSchema1 = List(
        StructField("name", StringType, true),
        StructField("greeting", StringType, false)
      )

      val expectedData1 = Seq(
        Row("titi", "hello world"),
        Row("foo", "hello world"),
        Row("toto", "hello world"),
        Row("tata", "hello world"),
        Row("bar", "hello world")
      )

      /* here we don't use the helper method toDF(). Because we don't want to have null value in greeting column*/
      val expectedDF1 = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData1),
        StructType(expectedSchema1)
      )

      //You can notice the order of rows affect the equality test.
      assertSmallDataFrameEquality(actualDF1, expectedDF1, orderedComparison = false)

    }

  }
}
