package org.pengfei

import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}
import org.scalatest.{FunSpec, TestSuite}


class TestColumnCreator
  extends FunSpec
    with ColumnComparer
    with SparkSessionTestWrapper {

  import spark.implicits._

  describe(".createColumnWithPower") {

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

  }

}
