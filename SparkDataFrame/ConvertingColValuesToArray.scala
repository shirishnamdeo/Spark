

val colValueArray = DataFrame.select($"column_name").rdd.map(r => r(0).asInstanceOf[String])


// And then selecting array of columns from DataFrame
DataFrame.select(colValueArray.head, colValueArray.tail: _*)



// Need to check
import org.apache.spark.sql.functions._

DataFrame.select(colValueArray.map(col): _*)
DataFrame.select(colValueArray:_*)
