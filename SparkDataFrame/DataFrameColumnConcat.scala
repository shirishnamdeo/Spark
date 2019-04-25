

DataFrame.withColumn("<newCOlName>", concat($"colName1", lit("<characterInBetween>"), $"colName2"))


DataFrame.withColumn("<newCOlName>", concat($"colName1", lit("<characterInBetween>"), $"colName2")).drop("colName1", "colName2")