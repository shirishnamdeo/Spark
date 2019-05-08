

val colValueArray = Array("column1", "column2", ...)

// And then selecting array of columns from DataFrame
DataFrame.select(colValueArray.head, colValueArray.tail: _*)



// Need to check
import org.apache.spark.sql.functions._

DataFrame.select(colValueArray.map(col): _*)  --Works!!

DataFrame.select(colValueArray:_*)    -- Doesn't works!!


// Is this even working?? 
DataFrame.select("*", concat($"<column_name1>", lit("<stringToAppend>")).alias("<alias_name>"))

DataFrame.select("*") -- Working!!