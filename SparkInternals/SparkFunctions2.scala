import org. apache. spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._


_____________________________________________________________________________________________________________________________________________________

// Creating Methods and Converting them to Function Objects

def count_null(dataFrame: DataFrame, col:String): String = {
    dataFrame.select(col).filter(dataFrame(col).isNull).count.toString
}
val func_count_null: (DataFrame, String) => String = count_null


def count_nan(dataFrame: DataFrame, col:String): String = {
    dataFrame.select(col).filter(dataFrame(col).isNaN).count.toString
}
val func_count_nan: (DataFrame, String) => String = count_nan


def count_empty_string(dataFrame: DataFrame, col:String): String = {
    dataFrame.select(col).filter(dataFrame(col) === "").count.toString
}
val func_count_empty_string: (DataFrame, String) => String = count_empty_string


def count_single_space(dataFrame: DataFrame, col:String): String = {
    dataFrame.select(col).filter(dataFrame(col) === " ").count.toString
}
val func_count_single_space: (DataFrame, String) => String = count_single_space




_____________________________________________________________________________________________________________________________________________________


//GenericFunction

def genericFunction(dataFrame: DataFrame, functionArray:Array[((DataFrame, String) => String, String, String)], columnList:Array[String]) = {
    val function_objects = functionArray.map(func => func._1)
    val function_names = functionArray.map(func => func._2)
    val function_dtypes = functionArray.map(func => func._3)
    val result_df_colNames = Seq("column_names") ++ function_names.toSeq
    val result_df_colTypes = Seq("string") ++ function_dtypes.toSeq
    val result_array = columnList.map(col => Array(col) ++ function_objects.map(func => func(dataFrame, col) ))
    val selectExprs = 0 until (functionArray.size + 1) map(i => $"temp".getItem(i).as(s"col$i"))
    val result_df = result_array.toSeq.toDF.withColumnRenamed("value” , “temp").select(selectExprs:_*).toDF(result_df_colNames: _*)
    val result_df2 = result_df.select((result_df_colNames zip result_df_colTypes).toSeq.map{case (column_name, 
    column_type) => col(column_name).cast(column_type)}: _*)
    result_df2
}


_____________________________________________________________________________________________________________________________________________________



val function1 = (func_count_null, "count_null", "bigint")
val function2 = (func_count_nan, "count_nan", "bigint")
val function3 = (func_count_empty_string, "count_empty_string", "bigint")
val function4 = (func_count_single_space, "count_single_space", "bigint")
val functionArray = Array(function1, function2, function3, function4)

val columnList = dataFrame.columns.filter(!_.contains("<column_to_exclude>"))



val result_df = genericFunction(dataFrame, functionArray, columnList)
result_df.printSchema
result_df.show()



_____________________________________________________________________________________________________________________________________________________



val data = Seq(
    (1, "col2_val1",  "col4_val1", 50),
    (2, "col2_val2",  "col4_val1", 12),
    (3, "col2_val3",  " ",         11),
    (4, null,         "col4_val1",  0),
    (5, "",           "col4_val1", 55)
)
data: Seq[(Int, String, String, Int)] = List((1,col2_val1,col4_val1,50), (2,col2_val2,col4_val1,12), (3,col2_val3," ",11), (4,null,col4_val1,0), (5,"",col4_val1,55))

val dataFrame = data.toDF("colName1", "colName2", "colName3", "colName4")

scala> dataFrame.show()
+--------+---------+---------+--------+
|colName1| colName2| colName3|colName4|
+--------+---------+---------+--------+
|       1|col2_val1|col4_val1|      50|
|       2|col2_val2|col4_val1|      12|
|       3|col2_val3|         |      11|
|       4|     null|col4_val1|       0|
|       5|         |col4_val1|      55|
+--------+---------+---------+--------+


val result_df = genericFunction(dataFrame, functionArray, columnList)


scala> result_df.printSchema
root
 |-- column_names: string (nullable = true)
 |-- count_null: long (nullable = true)
 |-- count_nan: long (nullable = true)
 |-- count_empty_string: long (nullable = true)
 |-- count_single_space: long (nullable = true)


scala> result_df.show()
+------------+----------+---------+------------------+------------------+
|column_names|count_null|count_nan|count_empty_string|count_single_space|
+------------+----------+---------+------------------+------------------+
|    colName1|         0|        0|                 0|                 0|
|    colName2|         1|        0|                 1|                 0|
|    colName3|         0|        0|                 0|                 1|
|    colName4|         0|        0|                 0|                 0|
+------------+----------+---------+------------------+------------------+









_____________________________________________________________________________________________________________________________________________________



val data = Seq(
    (1, "col2_val1", null, "col4_val1", 50),
    (2, "col2_val2", 4,    "col4_val1", 12),
    (3, "col2_val3", " ",  "col4_val1", null),
    (4, null,        1234, "col4_val1", Double.NaN),
    (5, "",          null, "col4_val1", 55)
)
// data: Seq[(Int, String, Any, String, Any)] = 
//  List((1,col2_val1,null,col4_val1,50), (2,col2_val2,4,col4_val1,12), (3,col2_val3," ",col4_val1,null), (4,null,1234,col4_val1,NaN), 
//      (5,"",null,col4_val1,55))


val dataFrame = data.toDF("colName1", "colName2", "colName3", "colName4", "colName5")
// ERROR - Because now our sequence object contains Null and NaN





