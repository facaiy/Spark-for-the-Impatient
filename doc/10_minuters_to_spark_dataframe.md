10 Minutes to DataFrame
=======================

Inspired by:
+ [10 Minutes to pandas](http://pandas.pydata.org/pandas-docs/stable/10min.html)
+ [hhbyyh/DataFrameCheatSheet](https://github.com/hhbyyh/DataFrameCheatSheet/blob/master/README.md)

### Requirement

1. Follow the [instructions](http://spark.apache.org/docs/latest/#downloading) to download spark.

2. To launch spark-shell, uncompress the file downloaded, change directory into it and execute the following command:

  ```bash
  ./bin/spark-shell
  ```

  Read the [instruction](http://spark.apache.org/docs/latest/#running-the-examples-and-shell) if you need more help.

3. Copy, paste and run the following code snippets when reading.

4. Customarily, we import as follows:

  ```scala
  import org.apache.spark.ml.linalg.{Vector, Vectors}
  import org.apache.spark.rdd.RDD
  import org.apache.spark.sql.{DataFrame, Row}
  import org.apache.spark.sql.functions._
  ```


### Object Creation

+ [API: Dataset](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset)

#### Primitive Type

```scala
import spark.implicits._

scala> val df = Seq((0, "Tom", 67.5, true), (1, "Lily", 45.3, false)).toDF("id", "name", "weight", "is_male")

scala> df.show
+---+----+------+-------+
| id|name|weight|is_male|
+---+----+------+-------+
|  0| Tom|  67.5|   true|
|  1|Lily|  45.3|  false|
+---+----+------+-------+
```

#### Case Class

```scala
import spark.implicits._

scala> case class Sample(id: Int, vector: org.apache.spark.ml.linalg.Vector)

scala> val df = Seq(Sample(0, Vectors.dense(1,3,5)), Sample(1,
Vectors.dense(2,4,6))).toDF("id", "vector")

scala> df.show
+---+-------------+
| id|       vector|
+---+-------------+
|  0|[1.0,3.0,5.0]|
|  1|[2.0,4.0,6.0]|
+---+-------------+
```

#### RDD and schema

```scala
import org.apache.spark.sql._
import org.apache.spark.sql.types._

scala> val values = Seq(Seq(0, "Tom", 67.5, true), Seq(1, "Lily", 45.3, false))

scala> val rows = values.map{x => Row(x:_*)}

scala> val rdd = spark.sparkContext.makeRDD[Row](rows)
ParallelCollectionRDD[4] at makeRDD at <console>:36

scala> val schema = StructType(StructField("id", IntegerType, false) :: StructField("name", StringType, false) :: StructField("weight", DoubleType, false) :: StructField("is_male", BooleanType, false) :: Nil)

scala> val df = spark.createDataFrame(rdd, schema)

scala> df.show
+---+----+------+-------+
| id|name|weight|is_male|
+---+----+------+-------+
|  0| Tom|  67.5|   true|
|  1|Lily|  45.3|  false|
+---+----+------+-------+

scala> df.dtypes
res5: Array[(String, String)] = Array((id,IntegerType), (name,StringType), (weight,DoubleType), (is_male,BooleanType))

scala> df.printSchema
root
 |-- id: integer (nullable = false)
 |-- name: string (nullable = false)
 |-- weight: double (nullable = false)
 |-- is_male: boolean (nullable = false)
```


### Viewing Data

```scala
scala> df.first
res8: org.apache.spark.sql.Row = [0,Tom,67.5,true]

scala> df.head(1)
res9: Array[org.apache.spark.sql.Row] = Array([0,Tom,67.5,true])

scala> df.take(1)
res11: Array[org.apache.spark.sql.Row] = Array([0,Tom,67.5,true])

scala> df.takeAsList(1)
res12: java.util.List[org.apache.spark.sql.Row] = [[0,Tom,67.5,true]]

scala> df.show(1)
+---+----+------+-------+
| id|name|weight|is_male|
+---+----+------+-------+
|  0| Tom|  67.5|   true|
+---+----+------+-------+
only showing top 1 row
```

Display the columns
```scala
scala> df.columns
res20: Array[String] = Array(id, name, weight, is_male)
```

Describe shows a quick statistic summary of your data
```scala
scala> df.describe().show
+-------+------------------+------------------+
|summary|                id|            weight|
+-------+------------------+------------------+
|  count|                 2|                 2|
|   mean|               0.5|              56.4|
| stddev|0.7071067811865476|15.697770542341358|
|    min|                 0|              45.3|
|    max|                 1|              67.5|
+-------+------------------+------------------+
```

Sorting by columns
```scala
scala> val df = Seq("a", "b", "c", "d", "e").zipWithIndex.toDF("word", "id")

scala> df.sort($"id".desc, $"word".asc).show
+----+---+
|word| id|
+----+---+
|   e|  4|
|   d|  3|
|   c|  2|
|   b|  1|
|   a|  0|
+----+---+
```

### Selection

#### Getting

##### Selection by Columns
```scala
scala> val df = Seq((0, "Tom", 67.5, true), (1, "Lily", 45.3, false)).toDF("id", "name", "weight", "is_male")

scala> df.select("id", "weight").show
+---+------+
| id|weight|
+---+------+
|  0|  67.5|
|  1|  45.3|
+---+------+

```

```scala
scala> val df = Seq((0, ("Tom", 36)), (1, ("Lucy", 25))).toDF("id", "info")

scala> df.show
+---+---------+
| id|     info|
+---+---------+
|  0| [Tom,36]|
|  1|[Lucy,25]|
+---+---------+

scala> df.printSchema
root
 |-- id: integer (nullable = false)
 |-- info: struct (nullable = true)
 |    |-- _1: string (nullable = true)
 |    |-- _2: integer (nullable = false)

scala> df.select($"info".getField("_1")).show
+-------+
|info._1|
+-------+
|    Tom|
|   Lucy|
+-------+

scala> df.select($"info._2").show
+---+
| _2|
+---+
| 36|
| 25|
+---+
```

More to see: [Working with Complex Data Formats with Structured Streaming in Apache Spark 2.1](https://getpocket.com/redirect?url=https://databricks.com/blog/2017/02/23/working-complex-data-formats-structured-streaming-apache-spark-2-1.html&formCheck=0c6cdc035692e1e335fd56e58e11987a)

##### Boolean Indexing
```scala
scala> df.where("id >= 1").show
+---+----+------+-------+
| id|name|weight|is_male|
+---+----+------+-------+
|  1|Lily|  45.3|  false|
+---+----+------+-------+

scala> df.where(df("id") >= 1).show
+---+----+------+-------+
| id|name|weight|is_male|
+---+----+------+-------+
|  1|Lily|  45.3|  false|
+---+----+------+-------+
```

#### Setting
```scala
// use native function
scala> df.withColumn("new_id", $"id" + 10).show
+---+----+------+-------+------+
| id|name|weight|is_male|new_id|
+---+----+------+-------+------+
|  0| Tom|  67.5|   true|    10|
|  1|Lily|  45.3|  false|    11|
+---+----+------+-------+------+

// use UDF
scala> val addUDF = udf{ (x: Int) => x + 10 }
scala> df.withColumn("new_id", addUDF($"id")).show
+---+----+------+-------+------+
| id|name|weight|is_male|new_id|
+---+----+------+-------+------+
|  0| Tom|  67.5|   true|    10|
|  1|Lily|  45.3|  false|    11|
+---+----+------+-------+------+

// drop
scala> df.drop("name").show
+---+------+-------+
| id|weight|is_male|
+---+------+-------+
|  0|  67.5|   true|
|  1|  45.3|  false|
+---+------+-------+

// rename
scala> df.withColumnRenamed("id", "old_id").show
+------+----+------+-------+
|old_id|name|weight|is_male|
+------+----+------+-------+
|     0| Tom|  67.5|   true|
|     1|Lily|  45.3|  false|
+------+----+------+-------+
```

### Missing Data
+ [API: DataFrameNaFunctions](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameNaFunctions)

```scala
scala> val rdd = sc.parallelize(
     |   Seq((null.asInstanceOf[Integer], null.asInstanceOf[Integer], Double.NaN, null),
     |       (new Integer(2), new Integer(2), 2.0, "F"),
     |       (new Integer(3), new Integer(3), 3.0, "N"),
     |       (new Integer(4), null.asInstanceOf[Integer], Double.NaN, "F")))

scala> val df = rdd.toDF("ACCT_ID", "M_CD", "C_CD","IND")

scala> df.show
+-------+----+----+----+
|ACCT_ID|M_CD|C_CD| IND|
+-------+----+----+----+
|   null|null| NaN|null|
|      2|   2| 2.0|   F|
|      3|   3| 3.0|   N|
|      4|null| NaN|   F|
+-------+----+----+----+
```

To drop any rows that have missing data.
```scala
scala> df.na.drop().show
+-------+----+----+---+
|ACCT_ID|M_CD|C_CD|IND|
+-------+----+----+---+
|      2|   2| 2.0|  F|
|      3|   3| 3.0|  N|
+-------+----+----+---+

scala> df.na.drop(how="all").show
+-------+----+----+---+
|ACCT_ID|M_CD|C_CD|IND|
+-------+----+----+---+
|      2|   2| 2.0|  F|
|      3|   3| 3.0|  N|
|      4|null| NaN|  F|
+-------+----+----+---+

scala> df.na.drop(2).show
+-------+----+----+---+
|ACCT_ID|M_CD|C_CD|IND|
+-------+----+----+---+
|      2|   2| 2.0|  F|
|      3|   3| 3.0|  N|
|      4|null| NaN|  F|
+-------+----+----+---+

scala> df.na.drop(Seq("C_CD")).show
+-------+----+----+---+
|ACCT_ID|M_CD|C_CD|IND|
+-------+----+----+---+
|      2|   2| 2.0|  F|
|      3|   3| 3.0|  N|
+-------+----+----+---+
```

Filling missing data
```scala
scala> df.na.fill(0).show
+-------+----+----+----+
|ACCT_ID|M_CD|C_CD| IND|
+-------+----+----+----+
|      0|   0| 0.0|null|
|      2|   2| 2.0|   F|
|      3|   3| 3.0|   N|
|      4|   0| 0.0|   F|
+-------+----+----+----+

scala> df.na.fill(Map("ACCT_ID" -> 0, "M_CD" -> 0, "C_CD" -> 0.0, "IND" -> " ")).show
+-------+----+----+---+
|ACCT_ID|M_CD|C_CD|IND|
+-------+----+----+---+
|      0|   0| 0.0|   |
|      2|   2| 2.0|  F|
|      3|   3| 3.0|  N|
|      4|   0| 0.0|  F|
+-------+----+----+---+
```

To replace value:
```scala
scala> df.na.replace("C_CD", Map(2.0 -> 999.9)).show
+-------+----+-----+----+
|ACCT_ID|M_CD| C_CD| IND|
+-------+----+-----+----+
|   null|null|  NaN|null|
|      2|   2|999.9|   F|
|      3|   3|  3.0|   N|
|      4|null|  NaN|   F|
+-------+----+-----+----+
```

To get the boolean mask where values are `nan`
```scala
scala> df.select(col("C_CD").isNaN).show
+-----------+
|isnan(C_CD)|
+-----------+
|       true|
|      false|
|      false|
|       true|
+-----------+
```

### Operations
#### stat
```scala
scala> val df = Seq((0, "Tom", 67.5, true), (1, "Lily", 45.3, false)).toDF("id", "name", "weight", "is_male")

scala> df.select(mean($"weight")).show
|avg(weight)|
+-----------+
|       56.4|
+-----------+

scala> df.select($"weight" - $"id").show
+-------------+
|(weight - id)|
+-------------+
|         67.5|
|         44.3|
+-------------+

scala> df.createTempView("table1")
scala> spark.sql("SELECT min(weight) FROM table1").show
+-----------+
|min(weight)|
+-----------+
|       45.3|
+-----------+
```

#### Apply
```scala
scala> val df = Seq((0, "Tom", 67.5, true), (1, "Lily", 45.3, false)).toDF("id", "name", "weight", "is_male")

scala> df.select("name").map(_.getString(0).length).show
+-----+
|value|
+-----+
|    3|
|    4|
+-----+

scala> df.map(_.getString(1).length).show
+-----+
|value|
+-----+
|    3|
|    4|
+-----+

scala> df.rdd.map(_.getString(1).length).collect
res30: Array[Int] = Array(3, 4)


scala> import org.apache.spark.sql.Row
scala> df.select("name").map{case Row(s: String) => s.length}.show
+-----+
|value|
+-----+
|    3|
|    4|
+-----+

scala> df.map{case Row(_, name: String, _, _) => name.length}.show
+-----+
|value|
+-----+
|    3|
|    4|
+-----+

scala> val stringLengthUDF = udf { (s: String) => s.length }
scala> df.select(stringLengthUDF($"name")).show
+---------+
|UDF(name)|
+---------+
|        3|
|        4|
+---------+

// operator on Vector
scala> case class Sample(id: Int, vector: org.apache.spark.ml.linalg.Vector)
scala> val df = Seq(Sample(0, Vectors.dense(1,3,5)), Sample(1, Vectors.dense(2,4,6))).toDF("id", "vector")
scala> df.show
+---+-------------+
| id|       vector|
+---+-------------+
|  0|[1.0,3.0,5.0]|
|  1|[2.0,4.0,6.0]|
+---+-------------+

scala> val vecSumUDF = udf { (v: org.apache.spark.ml.linalg.Vector) => v.toArray.sum }
scala> df.withColumn("vec_sum", vecSumUDF($"vector")).show
+---+-------------+-------+
| id|       vector|vec_sum|
+---+-------------+-------+
|  0|[1.0,3.0,5.0]|    9.0|
|  1|[2.0,4.0,6.0]|   12.0|
+---+-------------+-------+
```

### Merge
```scala
scala> val left = Seq(("foo", 1), ("bar", 2)).toDF("key", "lval")

scala> left.show
+---+----+
|key|lval|
+---+----+
|foo|   1|
|bar|   2|
+---+----+

scala> val right = Seq(("foo", 4), ("bar", 5), ("more", 6)).toDF("key", "rval")

scala> right.show
+----+----+
| key|rval|
+----+----+
| foo|   4|
| bar|   5|
|more|   6|
+----+----+

scala> left.join(right, "key").show
+---+----+----+
|key|lval|rval|
+---+----+----+
|foo|   1|   4|
|bar|   2|   5|
+---+----+----+

scala> left.join(right, Seq("key"), "outer").show
+----+----+----+
| key|lval|rval|
+----+----+----+
|more|null|   6|
| bar|   2|   5|
| foo|   1|   4|
+----+----+----+
```

### Grouping

+ [API: RelationalGroupedDataset](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.RelationalGroupedDataset)

```scala
scala> val df = Seq("a", "b", "c", "b", "a").zipWithIndex.toDF("value", "index")
scala> df.show
+-----+-----+
|value|index|
+-----+-----+
|    a|    0|
|    b|    1|
|    c|    2|
|    b|    3|
|    a|    4|
+-----+-----+

scala> df.groupBy("value").agg(sum($"index")).show
+-----+----------+
|value|sum(index)|
+-----+----------+
|    c|         2|
|    b|         4|
|    a|         4|
+-----+----------+
```


### Getting Data In/Out
+ [API: DataFrameReader](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameReader)
+ [API: DataFrameWriter](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameWriter)

#### CSV
```scala
// read
val df = spark.read.format("csv")
      .option("sep", "\t")
      .option("header", false)
      .option("inferSchema", "true")
      .option("nullValue", "\\N")
      .load(dataPath)

// write
scala> df.write.format("csv").save("/user/facai/test/csv")
```

#### libsvm
+ [API: LibSVMDataSource](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.source.libsvm.LibSVMDataSource)

```scala
// read
val df = spark.read.format("libsvm")
  .option("numFeatures", "780")
  .load("data/mllib/sample_libsvm_data.txt")

// write
// ref: https://stackoverflow.com/questions/41416291/how-to-prepare-data-into-a-libsvm-format-from-dataframe
scala> import org.apache.spark.ml.feature.LabeledPoint
scala> val pos = LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0))
scala> val neg = LabeledPoint(0.0, Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0)))
scala> val df = Seq(neg,pos).toDF("label","features")
scala> df.show
+-----+-------------------+
|label|           features|
+-----+-------------------+
|  0.0|(3,[0,2],[1.0,3.0])|
|  1.0|      [1.0,0.0,3.0]|
+-----+-------------------+

scala> df.write.format("libsvm").save("/user/facai/test/libsvm")
```

more to see: [Data Sources](https://spark.apache.org/docs/latest/sql-programming-guide.html#data-sources)