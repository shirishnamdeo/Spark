

DataFrame.filter($"<column_name>" > value)
DataFrame.filter($"<column_name>" >= value)
DataFrame.filter($"<column_name>" === value)
DataFrame.filter($"<column_name>" =!= value)





// Filtering Rows containing Null Value in a Column (Need to verify)

DataFrame.select($"<column_name>").filter(DataFrame("<column_name>").isNull).count()
Example: data.select($"requestor_id").filter(data("requestor_id").isNull).count()


DataFrame.select($"<column_name>").filter(DataFrame("<column_name>").isNaN).count()

DataFrame.select($"<column_name>").filter(DataFrame("<column_name>") === "").count()

DataFrame.filter(DataFrame("<column_name>").isNull || DataFrame("<column_name>").isNaN || DataFrame("<column_name>") === "" ).count()



DataFrame.filter(col("column_name").isNull))
DataFrame.filter(col("column_name").isNaN))
DataFrame.filter(col("column_name").isNotNull))




