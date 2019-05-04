

DataFrame.groupBy($"<column_name>").agg(sum("column_name2").as("<alias_name>")).show()



// Multiple operations can be applied insied the same .agg() function one after the another
DataFrame.groupBy($"<column_name>").agg(sum("column_name2").as("<alias_name>"), count("column_name3")).show()


