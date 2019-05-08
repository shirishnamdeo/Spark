ConcatWS

val friendsDF = Seq(
    (1, "Mallik", "Gandhamsetty"),
    (2, "Sandeep", "Makineni"),
    (3, "Pavan", "Soma")
).toDF("Id", "FirstName","LastName")


scala> friendsDF.show()
+---+---------+------------+
| Id|FirstName|    LastName|
+---+---------+------------+
|  1|   Mallik|Gandhamsetty|
|  2|  Sandeep|    Makineni|
|  3|    Pavan|        Soma|
+---+---------+------------+




scala> friendsDF.withColumn("concatCol", concat_ws(", ", $"Id", $"FirstName", $"LastName")).show(false)
// Note that the new column "concatCol" will be of type String
+---+---------+------------+-----------------------+
|Id |FirstName|LastName    |concatCol              |
+---+---------+------------+-----------------------+
|1  |Mallik   |Gandhamsetty|1, Mallik, Gandhamsetty|
|2  |Sandeep  |Makineni    |2, Sandeep, Makineni   |
|3  |Pavan    |Soma        |3, Pavan, Soma         |
+---+---------+------------+-----------------------+



scala> friendsDF.select(concat_ws(", ", $"Id", $"FirstName", $"LastName")).show()
scala> friendsDF.withColumn("concatCol", concat_ws(", ", $"Id", $"FirstName", $"LastName")).drop("Id", "FirstName", "LastName").show()
res38: org.apache.spark.sql.DataFrame = [concatCol: string]

+--------------------------------------+
|concat_ws(, , Id, FirstName, LastName)|
+--------------------------------------+
|                  1, Mallik, Gandha...|
|                  2, Sandeep, Makineni|
|                        3, Pavan, Soma|
+--------------------------------------+




import org.apache.spark.sql.functions.col
val colsToConcat = Seq(col("Id"), col("FirstName"), col("LastName"))

scala> friendsDF.select(concat_ws(", ", colsToConcat :_*)).show()
+--------------------------------------+
|concat_ws(, , Id, FirstName, LastName)|
+--------------------------------------+
|                  1, Mallik, Gandha...|
|                  2, Sandeep, Makineni|
|                        3, Pavan, Soma|
+--------------------------------------+

