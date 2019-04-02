
-- Adding column / Manipulating columns

DataFrame.select($"<column_name>").withColumn("my_col", col("<column_name>")).show()

DataFrame.select($"<column_name>").withColumn("my_col", split($"<column_name>", "-")).show() -- Return a array of elements

// Selecting the elements from the returned array from Split using getItem(N)
DataFrame.select($"<column_name>").withColumn("my_col", split($"<column_name>", "-")).select($"my_col".getItem(0).as("col1"), $"my_col".getItem(2).as("col2")).show()

-- Only the newly added columns is in result. How to get all the new and previous columns?
DataFrame.select($"<column_name>").withColumn("my_col", split($"<column_name>", "-")).select("<column_name>", $"my_col".getItem(0).as("col1"), $"my_col".getItem(2).as("col2")).show()

-- Clean Syntax
DataFrame.select($"<column_name>").withColumn("col1", split($"<column_name>", "-").getItem(0)).withColumn("col2", split($"<column_name>", "-").getItem(1)).show()

 
 