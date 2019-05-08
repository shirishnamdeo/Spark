// Concatenate Columns of DataFrame

lit(java.lang.Object literal)
// Creates a Column of literal value.


DataFrame.withColumn("new_column_name" , contat($"column_name1", lit(" "), $"column_name2"))
//Function contat() works in .withColumn() but not in select.

Example:
DataFrame.select(concat(col("<column_name1>"), col("<column_name2>")).alias("<new_column_name>")).show()

+------------------+
|                 a|
+------------------+
|MallikGandhamsetty|
|   SandeepMakineni|
|         PavanSoma|
+------------------+

DataFrame.select(concat(col("<column_name1>"), col("<column_name2>")).alias("<new_column_name>")).show()  -- This also works


DataFrame.select("*", concat(col("<column_name1>"), col("<column_name2>")).alias("<new_column_name>")).show()  -- But this doesn't works!!