
// Extracting the values of a column as a String
DataFrame.select($"column_name").rdd.map( row_obj => row_obj(0).asInstanceOf[String])
// This would still be a RDD

DataFrame.select($"column_name").rdd.map( row_obj => row_obj(0).asInstanceOf[String]).collect
// Will return an Array[String]


DataFrame.select($"column_name").collect.map( row => r(0).asInstanceOf[String])
DataFrame.select($"column_name").rdd.collect.map( row => r(0).asInstanceOf[String])
// Both above are same
// Collect can also be used before.
// I guess the only difference in the approaches would be that in the first one, the map and .asInstanceOf operation would be in executors, while in
// second one, the operations would be on Driver Node.



Array[Any].asInstanceOf[Array[String]] -- Doesn't work even if all the elements can possible be casted
Array[Any].map( item => item.asInstanceOf[String])  -- But this work to produce Array[String] !!



