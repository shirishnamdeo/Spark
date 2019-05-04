

DataFrame.drop("column_name1", "column_name2", ...)


// To 
val filteredColumns = DataFrame.columns.diff(column_array_to_exclude)
// column_array_to_exclude is a Array[String]