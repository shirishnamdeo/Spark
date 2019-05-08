

DataFrame.drop("column_name1", "column_name2", ...)
// Multiple Columns can be dropped at simultaneously. Comma(,) seperated list


val filteredColumns = DataFrame.columns.diff(column_array_to_exclude)
// column_array_to_exclude is a Array[String]


// To filter our one column
DataFrame.columns.filter(!_.contains("<column_name>"))


