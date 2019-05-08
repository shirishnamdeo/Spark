import org.apache.spark.sql.functions.explode

// Suppose we have a json formatted data with b containing multiple elements which we need to distribute/normalize.
scala> Seq("""{"a":1,"b":[2,3]}""")
res0: Seq[String] = List({"a":1,"b":[2,3]})


val dataFrame = spark.read.json(sc.parallelize(Seq("""{"a":1,"b":[2,3]}""")))
dataFrame: org.apache.spark.sql.DataFrame = [a: bigint, b: array<bigint>]


scala> dataFrame.printSchema
root
 |-- a: long (nullable = true)
 |-- b: array (nullable = true)
 |    |-- element: long (containsNull = true)


scala> dataFrame.show()
+---+------+
|  a|     b|
+---+------+
|  1|[2, 3]|
+---+------+



val flattened = dataFrame.withColumn("b", explode($"b"))
val flattened = dataFrame.withColumn("b", explode(col("b")))
flattened: org.apache.spark.sql.DataFrame = [a: bigint, b: bigint]


scala> flattened.show()
+---+---+
|  a|  b|
+---+---+
|  1|  2|
|  1|  3|
+---+---+


scala> flattened.printSchema
root
 |-- a: long (nullable = true)
 |-- b: long (nullable = true)


_____________________________________________________________________________________________________________________________________________________


// Another way to achieve similar result

val dataSet = Seq(
    (0, "Lorem ipsum dolor", 1.0, Array("prp1", "prp2", "prp3"))).
    toDF("id", "text", "value", "properties").
    as[(Integer, String, Double, scala.List[String])]
dataSet: org.apache.spark.sql.Dataset[(Integer, String, Double, List[String])] = [id: int, text: string ... 2 more fields]


scala> dataSet.show
+---+-----------------+-----+------------------+
| id|             text|value|        properties|
+---+-----------------+-----+------------------+
|  0|Lorem ipsum dolor|  1.0|[prp1, prp2, prp3]|
+---+-----------------+-----+------------------+


dataSet.flatMap{t => t._4.map{prp => (t._1, t._2, t._3, prp) }}.show()
// Reutrns a Dataset

scala> dataSet.flatMap{t => t._4.map{prp => (t._1, t._2, t._3, prp) }}.show()
+---+-----------------+---+----+
| _1|               _2| _3|  _4|
+---+-----------------+---+----+
|  0|Lorem ipsum dolor|1.0|prp1|
|  0|Lorem ipsum dolor|1.0|prp2|
|  0|Lorem ipsum dolor|1.0|prp3|
+---+-----------------+---+----+



_____________________________________________________________________________________________________________________________________________________



val data3 = Seq(
    (1, "A1", "B1", "C1", "D1"),
    (2, "A2", "B2", "C2", "D2"),
    (3, "A3", "B3", "C3", "D3")
).toDF("index", "col1", "col2", "col3", "col4")


scala> data3.show()
+-----+----+----+----+----+
|index|col1|col2|col3|col4|
+-----+----+----+----+----+
|    1|  A1|  B1|  C1|  D1|
|    2|  A2|  B2|  C2|  D2|
|    3|  A3|  B3|  C3|  D3|
+-----+----+----+----+----+



scala> data3.withColumn("newColumn", explode(array("col1", "col2", "col3", "col4"))).show()
+-----+----+----+----+----+---------+
|index|col1|col2|col3|col4|newColumn|
+-----+----+----+----+----+---------+
|    1|  A1|  B1|  C1|  D1|       A1|
|    1|  A1|  B1|  C1|  D1|       B1|
|    1|  A1|  B1|  C1|  D1|       C1|
|    1|  A1|  B1|  C1|  D1|       D1|
|    2|  A2|  B2|  C2|  D2|       A2|
|    2|  A2|  B2|  C2|  D2|       B2|
|    2|  A2|  B2|  C2|  D2|       C2|
|    2|  A2|  B2|  C2|  D2|       D2|
|    3|  A3|  B3|  C3|  D3|       A3|
|    3|  A3|  B3|  C3|  D3|       B3|
|    3|  A3|  B3|  C3|  D3|       C3|
|    3|  A3|  B3|  C3|  D3|       D3|
+-----+----+----+----+----+---------+


scala> data3.withColumn("newColumn", explode(array("col1", "col2", "col3", "col4"))).drop("col1", "col2", "col3", "col4").show()
scala> data3.select($"index", explode(array("col1", "col2", "col3", "col4")).as("newColumn")).show()
+-----+---------+
|index|newColumn|
+-----+---------+
|    1|       A1|
|    1|       B1|
|    1|       C1|
|    1|       D1|
|    2|       A2|
|    2|       B2|
|    2|       C2|
|    2|       D2|
|    3|       A3|
|    3|       B3|
|    3|       C3|
|    3|       D3|
+-----+---------+



// What if we only want a subset of column to explode
scala> data3.withColumn("newColumn", explode(array("col3", "col4"))).show()
+-----+----+----+----+----+---------+
|index|col1|col2|col3|col4|newColumn|
+-----+----+----+----+----+---------+
|    1|  A1|  B1|  C1|  D1|       C1|
|    1|  A1|  B1|  C1|  D1|       D1|
|    2|  A2|  B2|  C2|  D2|       C2|
|    2|  A2|  B2|  C2|  D2|       D2|
|    3|  A3|  B3|  C3|  D3|       C3|
|    3|  A3|  B3|  C3|  D3|       D3|
+-----+----+----+----+----+---------+
// Here only the specified column has exploded


_____________________________________________________________________________________________________________________________________________________

val df = sc.parallelize(Seq((1, Seq(2,3,4), Seq(5,6,7)), (2, Seq(3,4,5), Seq(6,7,8)), (3, Seq(4,5,6), Seq(7,8,9)))).toDF(“a”, “b”, “c”)
val df1 = df.select(df(“a”),explode(df(“b”)).alias(“b_columns”),df(“c”))
val df2 = df1.select(df1(“a”),df1(“b_columns”),explode(df1(“c”).alias(“c_columns”))).show()