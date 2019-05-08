import org.apache.spark.sql.types._


def extractStringColName(dataFrame: DataFrame, colType: Any): Array[String] = {
    val df_struct_field = dataFrame.schema.fields
    dataFrame.columns.map(col_name => (col_name, df_struct_field.filter(x => x.name == col_name)(0).dataType) ) filter (x => x._2 == colType) map ( x => x._1)
}



val timestampTypeCols = extractStringColName(dataFrame3, TimestampType)
val stringTypeCols    = extractStringColName(dataFrame3, StringType)
val shortTypeCols     = extractStringColName(dataFrame3, ShortType)
val integerTypeCols   = extractStringColName(dataFrame3, IntegerType)
val longTypeCols      = extractStringColName(dataFrame3, LongType)
val decimalTypeCols   = extractStringColName(dataFrame3, DecimalType(26,3))
val decimalTypeCols2  = extractStringColName(dataFrame3, DecimalType(7,4))

 
_____________________________________________________________________________________________________________________________________________________

[Development Phase]


DataFrame.schema
DataFrame.schema.fields
DataFrame.schema.fields(0)
DataFrame.schema.fields.map(x => dataType)
DataFrame.schema.fields(0).dataType


val df_struct_field = dataFrame.schema.fields

df_struct_field.filter(x => x.name == "<col_name>")
Array[org.apache.spark.sql.types.StructField]



def extractStringColName(dataFrame: DataFrame): Array[String] = {
    val df_struct_field = dataFrame.schema.fields
    dataFrame.columns.map(col_name => (col_name, df_struct_field.filter(x => x.name == col_name)(0).dataType) ) filter (x => x._2 == org.apache.spark.sql.types.StringType) map ( x => x._1)
}


def extractStringColName(dateframe: DataFrame, colType: org.apache.spark.sql.types.StringType): Array[String] = {
    val df_struct_field = dataFrame.schema.fields
    dataFrame.columns.map(col_name => (col_name, df_struct_field.filter(x => x.name == col_name)(0).dataType) )filter (x => x._2 == colType) map ( x => x._1)
}
// Works. So we can pass column type also in the function as a parameter


// For count of each type
dataFrame.columns.map(col_name => (col_name, df_struct_field.filter(x => x.name == col_name)(0).dataType)) map ( x => x._2) groupBy(identity) mapValues (_.length)


distinct_types.map(t => (t, extractStringColName(dataFrame, t))).toMap



_____________________________________________________________________________________________________________________________________________________

// All the different type of columns types in DataFrame

val df_struct_field = dataFrame.schema.fields
dataFrame.columns.map(col_name => (col_name, df_struct_field.filter(x => x.name == col_name)(0).dataType) filter (x => x._2) distinct



// To select sub-set of column we we have a column array 
DataFrame.select(columnArray.head, columnArray.tail: _*)

