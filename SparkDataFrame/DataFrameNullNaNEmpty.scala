https://www.mungingdata.com/apache-spark/dealing-with-null
https://stackoverflow.com/questions/45516387/how-to-find-columns-with-many-nulls-in-spark-scala?rq=1
https://stackoverflow.com/questions/39727742/how-to-filter-out-a-null-value-from-spark-dataframe

https://stackoverflow.com/questions/40500732/scala-dataframe-null-check-for-columns  [Advance   ]


https://stackoverflow.com/questions/33376571/replace-null-values-in-spark-dataframe



isNaN doesn't work on the TimestampType column I believe, atleast not  on ingestion_ts column



val sequenceObject3 = Seq(
    Row(1, "col1_val1", "col2_val1", "col3_val1"),
    Row(2, "col1_val2", "col2_val2", "col3_val2"),
    Row(3, "col1_val3", "col2_val3", "col3_val3"),
    Row(4, "col1_val4", "col2_val4", null),
    Row(5, "col1_val5", "col2_val5", "col3_val5"),
    Row(6, "",          "col2_val6", "col3_val6")
    )

val Schema3 = List(
    StructField("Number", IntegerType, true),
    StructField("Col1", StringType, true),
    StructField("Col2", StringType, true),
    StructField("Col3", StringType, true)
)

val dataframe3 = spark.createDataFrame(
    spark.sparkContext.parallelize(sequenceObject3),
    StructType(Schema3)
)



dataframe3.show()
+------+---------+---------+---------+
|Number|     Col1|     Col2|     Col3|
+------+---------+---------+---------+
|     1|col1_val1|col2_val1|col3_val1|
|     2|col1_val2|col2_val2|col3_val2|
|     3|col1_val3|col2_val3|col3_val3|
|     4|col1_val4|col2_val4|     null|
|     5|col1_val5|col2_val5|col3_val5|
|     6|         |col2_val6|col3_val6|
+------+---------+---------+---------+







// Filtering Out the Null 
DataFrame.filter(dF(colName).isNull).count()




// Filtering Rows containing Null Value in a Column (Need to verify)

DataFrame.select($"<column_name>").filter(DataFrame("<column_name>").isNull).count()

DataFrame.select($"<column_name>").filter(DataFrame("<column_name>").isNaN).count()

DataFrame.select($"<column_name>").filter(DataFrame("<column_name>") === "").count()

DataFrame.filter(DataFrame("<column_name>").isNull || DataFrame("<column_name>").isNaN || DataFrame("<column_name>") === "" ).count()






-----------------------------------------------------------------------------------------------------------------------------------------------------

DataFrame.filter($"<column_name>".isNotNull)
DataFrame.where(DataFrame("<column_name>").isNotNull)

DataFrame.filter($"<column_name>".isNull)
DataFrame.filter("<column_name> is null")

// To filter our the records which have Null values in <column_name>
DataFrame.filter(col("<column_name>").isNotNull)


DataFrame.filter(col("<column_name1>").isNotNull && col("<column_name2>").isNotNull)
DataFrame.where("<column_name1> is not null and <column_name2> is not null")


-- Finding all the columns with Not-Null values
val filterCond = DataFrame.columns.map(x=>col(x).isNotNull).reduce(_ && _)
DataFrame.filter(filterCond)


-- Dropping the row with any Null Values
Dataset<Row> filtered = DataFrame.filter(row -> !row.anyNull());


DataFrame.where(DataFrame.col("<column_name>").isNotNull().and(DataFrame.col("<column_name>").notEqual("")))



-----------------------------------------------------------------------------------------------------------------------------------------------------

Example:

case class Company(cName: String, cId: String, details: String)
case class Employee(name: String, id: String, email: String, company: Company)


// setting up example data

val e1 = Employee("n1", null, "n1@c1.com", Company("c1", "1", "d1"))
val e2 = Employee("n2", "2", "n2@c1.com", Company("c1", "1", "d1"))
val e3 = Employee("n3", "3", "n3@c1.com", Company("c1", "1", "d1"))
val e4 = Employee("n4", "4", "n4@c2.com", Company("c2", "2", "d2"))
val e5 = Employee("n5", null, "n5@c2.com", Company("c2", "2", "d2"))
val e6 = Employee("n6", "6", "n6@c2.com", Company("c2", "2", "d2"))
val e7 = Employee("n7", "7", "n7@c3.com", Company("c3", "3", "d3"))
val e8 = Employee("n8", "8", "n8@c3.com", Company("c3", "3", "d3"))

val employees = Seq(e1, e2, e3, e4, e5, e6, e7, e8)
val df = sc.parallelize(employees).toDF


df.show()
+----+----+---------+-----------+
|name|  id|    email|    company|
+----+----+---------+-----------+
|  n1|null|n1@c1.com|[c1, 1, d1]|
|  n2|   2|n2@c1.com|[c1, 1, d1]|
|  n3|   3|n3@c1.com|[c1, 1, d1]|
|  n4|   4|n4@c2.com|[c2, 2, d2]|
|  n5|null|n5@c2.com|[c2, 2, d2]|
|  n6|   6|n6@c2.com|[c2, 2, d2]|
|  n7|   7|n7@c3.com|[c3, 3, d3]|
|  n8|   8|n8@c3.com|[c3, 3, d3]|
+----+----+---------+-----------+



// Filter employees with 'null' id's
df.filter("id is null").show

df.filter("id is null").show
+----+----+---------+-----------+
|name|  id|    email|    company|
+----+----+---------+-----------+
|  n1|null|n1@c1.com|[c1, 1, d1]|
|  n5|null|n5@c2.com|[c2, 2, d2]|
+----+----+---------+-----------+



// Replacing Null Values with 0 else 1
// Essentially we are just overwritting the existing column (Obviously a new DF will be formed - Immutability)
df.withColumn("id", when($"id".isNull, 0).otherwise(1)).show

// Replacing Null Values with 0 else same
df.withColumn("id", when($"id".isNull, 0).otherwise($"id")).show


// Else additional column can also be formed
df.withColumn("id_updated", when($"id".isNull, 0).otherwise($"id")).show



df.filter($"id".isNull).show()
+----+----+---------+-----------+
|name|  id|    email|    company|
+----+----+---------+-----------+
|  n1|null|n1@c1.com|[c1, 1, d1]|
|  n5|null|n5@c2.com|[c2, 2, d2]|
+----+----+---------+-----------+


df.filter($"id".isNotNull).show()
+----+---+---------+-----------+
|name| id|    email|    company|
+----+---+---------+-----------+
|  n2|  2|n2@c1.com|[c1, 1, d1]|
|  n3|  3|n3@c1.com|[c1, 1, d1]|
|  n4|  4|n4@c2.com|[c2, 2, d2]|
|  n6|  6|n6@c2.com|[c2, 2, d2]|
|  n7|  7|n7@c3.com|[c3, 3, d3]|
|  n8|  8|n8@c3.com|[c3, 3, d3]|
+----+---+---------+-----------+









-----------------------------------------------------------------------------------------------------------------------------------------------------


scala> data_frame.select("age").filter("age is null").count()
res55: Long = 177

scala> data_frame.select("age").count()
res56: Long = 891

scala> data_frame.select("age").na.drop().count()
res57: Long = 714

scala> data_frame.select("age").na.fill(0).count()
res58: Long = 891

scala> data_frame.select("age").na.fill(0).filter("age is null").count()
res61: Long = 0




_____________________________________________________________________________________________________________________________________________________

Replacing (Creating a New DataFrame) Nulls, Nan's

[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameNaFunctions]



def fill(valueMap: Map[String, Any]): DataFrame
(Scala-specific) Returns a new DataFrame that replaces null values.

The key of the map is the column name, and the value of the map is the replacement value. 
The value must be of the following type: Int, Long, Float, Double, String, Boolean. 
Replacement values are cast to the column data type.

For example, the following replaces null values in column "A" with string "unknown", and null values in column "B" with numeric value 1.0.

df.na.fill(Map(
  "A" -> "unknown",
  "B" -> 1.0
))


