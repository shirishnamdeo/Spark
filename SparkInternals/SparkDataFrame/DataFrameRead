http://bailiwick.io/2017/08/08/selecting-dynamic-columns-in-spark-dataframes/
http://bailiwick.io/2017/08/09/joining-spark-dataframes-without-duplicate-column-names/
http://bailiwick.io/tag/spark/

https://www.mungingdata.com/apache-spark/dealing-with-null
https://www.mungingdata.com/





https://github.com/apache/spark/blob/d83c2F9F0b08d6d5d369d9fae04cdb15448e7f0d/sql/core/src/main/scala/org/apache/spark/sql/DataFrame.scala


1) Read external data (from files) into DataFrame


Spark 1.3 introduced a new DataFrame API as part of the Project Tungsten initiative which seeks to improve the performance and scalability of Spark. 
The DataFrame API introduces the concept of a schema to describe the data, allowing Spark to manage the schema and only pass data between nodes, in a much more efficient way than using Java serialization.

The DataFrame API is radically different from the RDD API because it is an API for building a relational query plan that Spark’s Catalyst optimizer can then execute.

Limitations : 
    Because the code is referring to data attributes by name, it is not possible for the compiler to catch any errors. If attribute names are incorrect then the error will only detected at runtime, when the query plan is created.

    Another downside with the DataFrame API is that it is very scala-centric and while it does support Java, the support is limited.



-----------------------------------------------------------------------------------------------------------------------------------------------------


val DIR_PATH = "file:///D:/SoftwareInstalled/Spark/Spark240/spark-2.4.0-bin-hadoop2.7/examples/src/main/resources/"


val CSV_FILENAME = "people.csv"
val JSON_FILENAME = "employees.json"
val TEXT_FILENAME = "people.txt"
val AVRO_FILENAME = "users.avro"
val AVSC_FILENAME = "users.avsc"    -- Avro Schema File Format
val ORC_FILENAME = "users.orc"
val PARQUET_FILENAME = "users.parquet"




val csv_file =  DIR_PATH + CSV_FILENAME
spark.read.csv(csv_file)  -- Reading as a Dataframe


val json_file = DIR_PATH + JSON_FILENAME
spark.read.json(json_file)


val text_file =  DIR_PATH + TEXT_FILENAME
spark.read.text(text_file)


val avro_file = DIR_PATH + AVRO_FILENAME
spark.read.avro(avro_file)


val avsc_file = DIR_PATH + AVSC_FILENAME
** Doesn't exists


val orc_file =  DIR_PATH + ORC_FILENAME
spark.read.orc(orc_file)


val parquet_file = DIR_PATH + PARQUET_FILENAME
spark.read.parquet(parquet_file)





import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};

val customSchema = StructType(Seq(StructField("userId", StringType, true), StructField("color", StringType, true), StructField("count", IntegerType, true)))
// A schema corresponds to a StructType with some StructFields. The StructFields are typed and can be nullable. 
// We define a "userId" as a nullable string type, a "color" as a nullable string type and a "count" as a nullable integer type.

val readAsCsvFormat = spark.read.schema(customSchema).option("delimiter", ",").csv("/user/nsaby/testevent")


spark.read.option("sep", "\u0001").text("<path_to_DIR_or_FILE>")

    .option("sep", "\u0001")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .option("header", "true")

    .format("[text|csv]").load(<"path_to_DIR_or_FILE">)  [If format is specified, then we use just load()]

    



DataFrame.repartition(number)
-- Return a new dataframe (replica) with updated partition. Existing Partition will not be changed


DataFrame.printSchema
DataFrame.explain
Dataframe.describe().show()
DataFrame.dtypes
Dataframe.show(numRows, <true/false>)    -- true/false for truncating/not-truncating the output



Observation:
    Initially the Dataframe has N partitions, but during the selection of some columns some columns get excluded/dropped.
    It may happen than even after the dropping of columns, the number of partitions remains same (won't decrease)
    If we save this Dataframe, then the number of files created (say parquet files), will be same as number of Partitions in the DataFrame.
    But, when reading the data again, the number of partitions changes(decreases), even thought there was N parquet files in directory.

 



 
-----------------------------------------------------------------------------------------------------------------------------------------------------

2. Loading Data in Dataframe From Table (Hive)

spark.sql(SELECT * FROM <namespace>.<table_name>)