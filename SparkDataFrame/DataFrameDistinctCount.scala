
// Distinct values if a specific column
DataFrame.select(col("<column-name>")).distinct.show


import org.apache.spark.sql.functions.count

// Count for each distinct value of a column of a DataFrame

DataFrame.select(col("column-name")).groupBy("<column-name>").agg("column-name").show()


// DataFrame Distinct Row Count
DataFrame.distinct.count 


// How to use this function
import org.apache.spark.sql.functions.countDistinct