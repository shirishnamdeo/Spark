import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.approx_count_distinct --If Speed is more important


DataFrame.agg(countDistinct("<column_name>")).show()   -- Gives a distinct of a particular column
DataFrame.select("<column_name>").distinct.count       -- Gives a distinct of a particular column

DataFrame.agg(approx_count_distinct(<column_name>)).show()

DataFrame_Columns.foreach(col => println(DataFrame.agg(countDistinct(col)).show())) -- works, but gives in wierd format.

DataFrame.foreach(col => println(col, DataFrame.select(col).distinct.count))


// Function to count distinct values of a DataFrame and Return a DataFrame
// Why this function is not LAZY. It evaluates the result as soon as it is called. 
def count_distinct(data_frame:org.apache.spark.sql.DataFrame) : org.apache.spark.sql.DataFrame = {
    val df_columns = data_frame.columns
    
    // 1. Calculating Distinct Count of each columns
    // 2. Converting the result to Map -> Sequence -> DataFrame
    df_columns.map(col => (col, data_frame.select(col).distinct.count)).toMap.toSeq.toDF("column_name","distinct_count")
}


-- Another method
val exprs = DataFrame.columns.map((_ -> "approx_count_distinct")).toMap
DataFrame.agg(exprs).show()




---------------------------------------------------------------------------------------------------

def count_null(data_frame:org.apache.spark.sql.DataFrame) : org.apache.spark.sql.DataFrame = {
    val df_columns = data_frame.columns
    df_columns.map(col => (col, data_frame.select(col).filter(data_frame(col).isNull).count)).toMap.toSeq.toDF("column_name","null_count")
}



def count_nan(data_frame:org.apache.spark.sql.DataFrame) : org.apache.spark.sql.DataFrame = {
    val df_columns = data_frame.columns
    df_columns.map(col => (col, data_frame.select(col).filter(data_frame(col).isNaN).count)).toMap.toSeq.toDF("column_name","nan_count")
}



def count_empty_string(data_frame:org.apache.spark.sql.DataFrame) : org.apache.spark.sql.DataFrame = {
    val df_columns = data_frame.columns
    df_columns.map(col => (col, data_frame.select(col).filter(data_frame(col) === "").count)).toMap.toSeq.toDF("column_name","empty_string_count")
}




def count_single_space(data_frame:org.apache.spark.sql.DataFrame) : org.apache.spark.sql.DataFrame = {
    val df_columns = data_frame.columns
    df_columns.map(col => (col, data_frame.select(col).filter(data_frame(col) === " ").count)).toMap.toSeq.toDF("column_name","single_space_count")
}





