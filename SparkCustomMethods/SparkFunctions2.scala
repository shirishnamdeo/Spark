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


def null_fraction(dataFrame: DataFrame, col:String): String = {
    "%3f".format(dataFrame.select(col).filter(dataFrame(col).isNull).count.toDouble/dataFrame.count()).toString
}
val func_null_fraction: (DataFrame, String) => String = null_fraction


def nan_fraction(dataFrame: DataFrame, col:String): String = {
    "%3f".format(dataFrame.select(col).filter(dataFrame(col).isNaN).count.toDouble/dataFrame.count()).toString
}
val func_nan_fraction: (DataFrame, String) => String = nan_fraction 


def empty_string_fraction(dataFrame: DataFrame, col:String): String = {
    "%3f".format(dataFrame.select(col).filter(dataFrame(col) === "").count.toDouble/dataFrame.count()).toString
}
val func_empty_string_fraction: (DataFrame, String) => String = empty_string_fraction


def single_space_fraction(dataFrame: DataFrame, col:String): String = {
    "%3f".format(dataFrame.select(col).filter(dataFrame(col) === " ").count.toDouble/dataFrame.count()).toString
}
val func_single_space_fraction: (DataFrame, String) => String = single_space_fraction


def distinct_count(dataFrame: DataFrame, col:String): String = {
    dataFrame.select(col).distinct.count.toString
}
val func_distinct_count: (DataFrame, String) => String = distinct_count


def mean_value(dataFrame: DataFrame, col_name:String): String = {
    "%.4f".format(dataFrame.select(mean(col(col_name))).collect()(0)(0)).toString
}
val func_mean_value: (DataFrame, String) => String = mean_value


def std_value(dataFrame: DataFrame, col_name:String): String = {
    "%.4f".format(dataFrame.select(stddev(col(col_name))).collect()(0)(0)).toString
}
val func_std_value: (DataFrame, String) => String = std_value


def max_value(dataFrame: DataFrame, col_name:String): String = {
    "%.4f".format(dataFrame.select(max(col(col_name))).collect()(0)(0).toString.toDouble).toString
}
val func_max_value: (DataFrame, String) => String = max_value
// In above methods defination, .toString.toDouble is kept there because for some columns data types it would be a double while for some other column_name
// column type it would be INT. So keeping a coherent type for every outcome.

def min_value(dataFrame: DataFrame, col_name:String): String = {
    "%.4f".format(dataFrame.select(min(col(col_name))).collect()(0)(0).toString.toDouble).toString
}
val func_min_value: (DataFrame, String) => String = min_value


def sum_value(dataFrame: DataFrame, col_name:String): String = {
    "%.4f".format(dataFrame.select(sum(col(col_name))).collect()(0)(0).toString.toDouble).toString
}
val func_sum_value: (DataFrame, String) => String = sum_value



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
    val result_df = result_array.toSeq.toDF.withColumnRenamed("value" , "temp").select(selectExprs:_*).toDF(result_df_colNames: _*)
    val result_df2 = result_df.select((result_df_colNames zip result_df_colTypes).toSeq.map{case (column_name, 
    column_type) => col(column_name).cast(column_type)}: _*)
    result_df2
}


// Improvement: 
// dataDF2.select(dataDF2.columns.filter(colName => colsToSelect.contains(colName)).map(colName => new Column(colName)): _*).show()



_____________________________________________________________________________________________________________________________________________________



val function1 = (func_count_null,           "count_null",       "bigint")
val function2 = (func_count_nan,            "count_nan",        "bigint")
val function3 = (func_count_empty_string,   "count_empty_string", "bigint")
val function4 = (func_count_single_space,   "count_single_space", "bigint")
val function5 = (func_null_fraction,        "null_fraction",    "double")
val function6 = (func_nan_fraction,         "nan_fraction",     "double")
val function7 = (func_empty_string_fraction,"empty_string_fraction", "double")
val function8 = (func_single_space_fraction,"single_space_fraction", "double")
val function9 = (func_distinct_count,       "distinct_count",   "bigint")
val function10 = (func_mean_value,          "mean_value",       "double")
val function11 = (func_std_value,           "std_value`",       "double")
val function12 = (func_max_value,           "max_value",        "double")
val function13 = (func_min_value,           "min_value",        "double")
val function14 = (func_sum_value,           "sum_value",        "double")

val functionArray = Array(function1, function2, function3, function4, function5, function6, function7, function8, function9)

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
_____________________________________________________________________________________________________________________________________________________


[Development Phase]

https://stackoverflow.com/questions/41341724/spark-scala-column-type-determine


// GenericFunction Long type Variant
def genericFunction(dataFrame: DataFrame, functionArray:Array[(DataFrame, String) -> Long], columnList:Array[String]) = {
    columList.map(col => Array(col) ++ functionArray.map(func => func(dataFrame, col) ))
    // functionArray is an array, thus it will return an Array of values (presuming that func will return raw values)
}


genericFunction(dataFrame, Array(func_count_null), dataFrame.columns)
genericFunction(dataFrame, Array(func_count_null, func_count_nan), dataFrame.columns.filter(!_.contains("ingestion_ts")))




// GenericFunction String type Variant
def genericFunction(dataFrame: DataFrame, functionArray:Array[(DataFrame, String) -> String], columnList:Array[String]) = {
    columList.map(col => Array(col) ++ functionArray.map(func => func(dataFrame, col) ))
    // functionArray is an array, thus it will return an Array of values (presuming that func will return raw values)
}

genericFunction(dataFrame, Array(func_count_null, func_count_nan), dataFrame.columns.filter(!_.contains("ingestion_ts")))
-- Returns: Array[Array[String]]


-- ERROR in converting into dataframe
scala> Array(Array("stringl", 0, 0), Array("string2", 0, 0))
res38: Array[Array[Any]] = Array(Array(stringl, 0, 0), Array(string2, 0, 0))
-- Above, is the pain point. The Output of the genericFunction is Array[Array[Any]], to which we are not able to
type cast into proper format
-- to be moulded into a DataFrame


-------------------------------------------------------------------------------------------------------


val a = genericFunction(df, Array(func_count_null, func_count_nan, func_count_empty_string,
func_count_single_space), df.columns.filter(!_.contains("ingestion_ts")) )
-- Array[Array[String]]

val selectExprs = 0 until 5 map (i => $"temp".getItem(i).as(s"col$i"))
a.toSeq.toDF.withColumnRenamed("value” , “temp").select(selectExprs:_*).show(50)



--------------------------------------------------------------------------------------------------------




 
val result_df = genericFunction(dataFrame, functionArray, columnList)
result_df.printSchema
result_df.show(50)


import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType

 

function_names.foreach(name => result_df.withColumn("_"+name, result_df.col(name).cast(DoubleType)).drop(name) .withColumnRenamed("_"+name, name))
// How to keep iteratively apply this so to change the DataFrame itselt

val newNames = Seq("id", "x1", "x2", "x3", "x4")
val dfRenamed = result_df.toDF(newNames: _*)
// The above wilecard characted can only be passed in a parameter
result_df.select(function_names.map{ column_name => col(column_name).cast("double")}: _*).printSchema

result_df.select((result_df_colNames zip result_df_colTypes).map{(column_name, column_type) => col(column_name).cast(column_type)}: _*).printSchema



_________________________________________________________________________________________


val df = sc.parallelize(Seq(
  ("foo", "1.0", "2", "true"),
  ("bar", "-1.0", "5", "false")
)).toDF("v", "x", "y", "z")

val types = Seq(
  ("v", "string"), ("x", "double"), ("y", "bigint"), ("z", "boolean")
)

df.select(types.map{case (c, t) => col(c).cast(t)}: _*)


___________________________________________________________________________________



result_df.select((result_df_colNames zip result_df_colTypes).map{(column_name, column_type) => col(column_name).cast(column_type)}: _*).printSchema
// Not working



scala> (result_df_colNames zip result_df_colTypes).toSeq.map{case (k,v) => k}
res65: Seq[String] = List(column_names, count_null, count_nan, count_empty string, count_single_space)

result_df.select((result_df_colNames zip result_df_colTypes).toSeq.map{case (column_name, column_type) => col(column_name).cast(column_type)}: _*).printSchema




____________________________________________________________________________________


import shapeless._
import syntax.std.tuple._



// Isn't working!
def arrayToTuple(array: Array[Int]) = {
    var my_tuple = ()
    for( ele <- array) {
    my_tuple = my tuple :+ (ele)
    }
    my_tuple
)


______________________________________





scala> Seq((0,0,0), (1,1,1))
res27: Seq[(Int, Int, Int)] = List((0,0,0), (1,1,1))

scala> Seq(Array(0,0,0), Array(1,1,1)).toDF.show()

 

scala> Seq(Array(Array(0,0,0), Array(1,1,1))).toDF.show()
<console>:29: error: value toDF is not a member of Seq[Array[Array[Int]]]
Seq(Array(Array(®,@,0), Array(1,1,1))).toDF.show()


________________________

scala> (1,2,3) :+ (6)
res9: (Int, Int, Int, Int) = (1,2,3,6)





