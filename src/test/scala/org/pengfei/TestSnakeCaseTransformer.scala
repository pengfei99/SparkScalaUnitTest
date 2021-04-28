package org.pengfei

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.FunSpec

class TestSnakeCaseTransformer extends FunSpec with DataFrameComparer with SparkSessionTestWrapper {
  import spark.implicits._

  /* Test the method snakeCase*/
  describe(".snakeCase") {

    it("downcases uppercase letters") {
      assert(SnakeCaseTransformer.snakeCase("HeLlO") === "hello")
    }

    it("converts spaces to underscores") {
      assert(SnakeCaseTransformer.snakeCase("Hi There") === "hi_there")
    }

  }
/* Test the method snakeCaseColumns*/
  describe(".snakeCaseColumns") {

    it("snake_cases the column names of a DataFrame") {

      val sourceDF = Seq(
        ("funny", "joke")
      ).toDF("A b C", "de F")

      val actualDF = SnakeCaseTransformer.snakeCaseColumns(sourceDF)

      val expectedDF = Seq(
        ("funny", "joke")
      ).toDF("a_b_c", "de_f")
      print("source df:")
      sourceDF.show()
      print("transformed df:")
      actualDF.show()
      print("expected df:")
      expectedDF.show()
      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }
}
