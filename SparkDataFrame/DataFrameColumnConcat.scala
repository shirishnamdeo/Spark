

DataFrame.withColumn("<newCOlName>", concat($"colName1", lit("<characterInBetween>"), $"colName2"))


DataFrame.withColumn("<newCOlName>", concat($"colName1", lit("<characterInBetween>"), $"colName2")).drop("colName1", "colName2")




// Operations in .withColumn
DataFrame.withColumn("<newCOlName>", $"colName1" === $"colName2")
DataFrame.withColumn("<newCOlName>", when($"colName1" === $"colName2", 1).otherwise(0))

DataFrame.withColumn("<newCOlName>", myUDF($"colName1"))  -- A UDF can also be applied


DataFrame.withColumn("<newCOlName>", $"colName1" + $"colName2")
DataFrame.withColumn("<newCOlName>", $"colName1" - $"colName2")
DataFrame.withColumn("<newCOlName>", $"colName1" * $"colName2")
DataFrame.withColumn("<newCOlName>", $"colName1" / $"colName2")
