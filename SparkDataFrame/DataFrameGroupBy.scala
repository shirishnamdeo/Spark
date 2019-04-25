DataFrame.groupBy("col1").sum("col2", "col3")
DataFrame.groupBy("col1").agg(sum("col2").alias("col2"), sum("col3").alias("col3"))




// Count of each value of a column
DataFrame.select(col("column-name")).groupBy("column-name").agg(count"column-name").show()
// Observation: IF the column contains 'null' values, then the 'null' will appear as a value in the grouping, but the count will remain 0



// Learn how to use this
groupBy(identity)

