// User-Defined Functions (aka UDF) is a feature of Spark SQL to define new Column-based functions that extend the vocabulary of Spark SQL’s DSL for
// transforming Datasets.

// You define a new UDF by defining a Scala function as an input parameter of udf function. It accepts Scala functions of up to 10 input parameters.


_____________________________________________________________________________________________________________________________________________________


val dataframe = Seq((0, "hello"), (1, "world")).toDF("id", "text")

// Define a regular Scala function
val upper: String => String = _.toUpperCase


import org.apache.spark.sql.functions.udf

// Define a UDF that wraps the upper Scala function defined above
// You could also define the function in place, i.e. inside udf but separating Scala functions from Spark SQL's UDFs allows for easier testing.

val upperUDF = udf(upper)
// upperUDF: org.apache.spark.sql.expressions.UserDefinedFunction

dataframe.withColumn("upper", upperUDF('text)).show

scala> dataframe.withColumn("upper", upperUDF('text)).show
+---+-----+-----+
| id| text|upper|
+---+-----+-----+
|  0|hello|HELLO|
|  1|world|WORLD|
+---+-----+-----+




// You could have also defined the UDF this way
val upperUDF = udf { s: String => s.toUpperCase }

// or even this way
val upperUDF = udf[String, String](_.toUpperCase)




_____________________________________________________________________________________________________________________________________________________


// You can register UDFs to use in SQL-based query expressions via UDFRegistration (that is available through SparkSession.udf attribute).

scala> spark.udf.register("myUpper", (input: String) => input.toUpperCase)
// You can query for available standard and user-defined functions using the Catalog interface (that is available through SparkSession.catalog 
// attribute).

scala> spark.catalog.listFunctions.filter('name like "%upper%").show(false)
// This will list the all the functions available.