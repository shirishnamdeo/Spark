[https://stackoverflow.com/questions/40800920/how-do-i-convert-arrayrow-to-dataframe]



DataFrame.take(N)  -- Returns an Array[Row]
DataFrame.limit(N) -- Returns an Array[Row]
DataFrame.first    -- Returns a Row




val schema = StructType(
    StructField("text", StringType, false) ::
    StructField("y", IntegerType, false) :: Nil)

val arr = df1.head(3); // Array[Row]

val dfFromArray = sqlContext.createDataFrame(sparkContext.parallelize(arr), schema);


// You can also map parallelized array and cast every row:
val dfFromArray = sparkContext.parallelize(arr).map(row => (row.getString(0), row.getInt(1)))
    .toDF("text", "y");


// In case of one row, you can run:
val dfFromArray = sparkContext.parallelize(Seq(row)).map(row => (row.getString(0), row.getInt(1)))
    .toDF("text", "y");


In Spark 2.0 use SparkSession instead of SQLContext.




_____________________

scala> val value=d.take(1)
value: Array[org.apache.spark.sql.Row] = Array([1,3])

scala> val asTuple=value.map(a=>(a.getInt(0),a.getInt(1)))
asTuple: Array[(Int, Int)] = Array((1,3))

scala> sc.parallelize(asTuple).toDF
res6: org.apache.spark.sql.DataFrame = [_1: int, _2: int]



_____________________

If you have List<Row>, then it can directly be used to create a dataframe or dataset<Row> using spark.createDataFrame(List<Row> rows, StructType schema). Where spark is SparkSession in spark 2.x



