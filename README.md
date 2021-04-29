# SparkScalaUnitTest

This project aims to show you how to do unit test on spark(scala) project. For pyspark project, we will show you how in 
another project. The unit testing can help you to avoid run sample data sets to develop code on a cluster. Because
clusters are slow to start and may cost you money. 


## 0. Background
We found two framework which can help us to do unit testing on spark project.
1. spark-fast-tests: https://github.com/MrPowers/spark-fast-tests
2. spark-testing-base: https://github.com/holdenk/spark-testing-base

In this project, we use the **spark-fast-tests** framework, because the **spark-testing-base** has not been updated for a while.

## 1. Installation
Unfortunately, the **spark-fast-tests** framework can not work alone. It heavily relies on **scalatest** 
(https://www.scalatest.org/) framework. 

And scalatest framework has many requirements of your project source file organization in order to do unit testing 
correctly. And setting it up manually is very difficult. So the easiest way to integrate it in your project is to 
use maven to build a scala project with the scalatest support.

We suppose you already have maven on your pc. To make it simple, we use an archetype which is provided by the **Scala 
Maven Plugin** 

```shell script
mvn archetype:generate -DarchetypeGroupId=net.alchim31.maven -DarchetypeArtifactId=scala-archetype-simple

# When it’s done, you should see a new folder named with the artifactId(project name). cd into it and run
mvn package
```

Then you need to complete your project pom file. You can check the pom file of this project to have some insights.

## 2. Test your functions
In spark, we normally has two type of functions:
1. transform a data frame by adding some new columns.
2. create a new data frame based on one or more source data frames.

So basically, we have two types of test:
1. Column equality test: Check if the generated column value equals the expected column value
2. Data frame equality test: Check if the generated data frame equal the expected data frame.

### 2.1 Column equality test
You can find the function **createColumnWithPower** in Object **src.main.scala.org.pengfei.ColumnCreator**. 
It takes a digit column and generate a new column which is the power 2 of the source column.
In unit test class **src.test.scala.org.pengfei.TestColumnCreator**. We test the function **createColumnWithPower**.

We mainly used two method:
1. assertColumnEquality : general equality test on any column type
2. assertDoubleTypeColumnEquality : if column type is float or double, we may have precision issues. With this method, 
         we can set a precision for the values which we want to compare. 

### 2.2 Data frame equality test
To test equality of two data frames, we use two method:
1. assertSmallDatasetEquality : It is faster for test DataFrames that locates on your local machine. 
2. assertLargeDatasetEquality : It is more optimal for DataFrames that are split across nodes in a cluster.

#### 2.2.1 Data frame schema mismatch
These two method check first the equality of the schema. As the schema of spark has three properties:
1. Column Name
2. Column type
3. Nullable
If one of the properties mismatch, you will receive a data frame mismatch error. And based on how your dataframes
are created, **the Nullable properties can be true or false**. In most of time, it does not impact the equality
of data frames. 

To ignore the nullable flag 
```scala
assertSmallDatasetEquality(sourceDF, expectedDF, ignoreNullable = true)
```

#### 2.2.2 Unordered DataFrame equality comparisons

For most of the time, the row order does not affect the equality of two data frames. For example, DF1 should equal DF2.
But if we just use **assertSmallDatasetEquality**, it will return a mismatch error
```shell script
# DF1:
+------+
|number|
+------+
|     1|
|     5|
+------+
# DF2:
+------+
|number|
+------+
|     5|
|     1|
+------+
``` 
So we want to ignore the row order, we can set the **orderedComparison** boolean flag to false and 
spark-fast-tests will sort the DataFrames before performing the comparison.

```scala
assertSmallDataFrameEquality(sourceDF, expectedDF, orderedComparison = false)
```     
For complete code example, please check **src/test/scala/TestAddGreetings**                           
#### 2.2.3 Approximate DataFrame Equality
As we mentioned before, if the data frame has float or double column, when we compare them, we need to specify a
precision. For example, if we compare the two below data frame.  

```shell script
# DF1
+------+-------+
|   1.0|    1.0|
|   2.0|    4.0|
|   3.0|    9.0|
|   4.0|   16.0|
+------+-------+
# DF2
|source|power_2|
+------+-------+
|   1.0|   1.01|
|   2.0|   4.06|
|   3.0|  9.001|
|   4.0| 16.001|
+------+-------+

```


```scala
// If set precision to 0.1, this will return true
assertApproximateDataFrameEquality(DF1, DF2, 0.1,ignoreNullable = true)

//If set precision to 0.1, this will return false
assertApproximateDataFrameEquality(DF1, DF2, 0.01,ignoreNullable = true)
```

For complete code example, please check **src/test/scala/TestColumnCreator** 

## 3. Creat SparkSession for your test environment

The spark-fast-tests framework doesn't provide a SparkSession object in your test suite, so you'll need to make 
one yourself.

```scala
import org.apache.spark.sql.SparkSession
trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("spark session for test env")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

}
```

Note in your local test environment, it's better to set the number of shuffle partitions to a small number like one 
or four. This configuration can make your tests run up to 70% faster. 

**Don't use this SparkSession configuration when you're working with big DataFrames in your test suite or running 
production code.**