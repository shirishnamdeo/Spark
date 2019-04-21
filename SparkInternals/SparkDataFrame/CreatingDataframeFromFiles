1. DataFrame from CSV files (when the file doesn't contain the Header/Schema)
2. 
3. Verify correctness of the DataFrame



_____________________________________________________________________________________________________________________________________________________


DataFrame from CSV files (when the file doesn't contain the Header/Schema)

[https://docs.databricks.com/spark/latest/data-sources/read-csv.html]


// Reading CSV file without the header information.

spark.read.csv("<data_file_path>")
// Spark DataFrame will be created but without the Named columns, instead columns will be names like _c0, _cl etc.




// We can also specify the schema while reading the data into DataFrame itself using .schema() method


import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DoubleType


// Example From DataBrick's Diamond data
val schema = StructType(
	Seq(
		StructField(name = "longitude", 		dataType = DoubleType, nullable = false),
		StructField(name = "latitude", 			dataType = DoubleType, nullable = false),
		StructField(name = "housingMediandge", 	dataType = DoubleType, nullable = false),
		StructField(name = "totalRooms", 		dataType = DoubleType, nullable = false),
		StructField(name = "totalBedrooms",  	dataType = DoubleType, nullable = false),
		StructField(name = "population", 		dataType = DoubleType, nullable = false),
		StructField(name = "households", 		dataType = DoubleType, nullable = false),
		StructField(name = "medianIncome", 		dataType = DoubleType, nullable = false),
		StructField(name = "medianHouseValue", 	dataType = DoubleType, nullable = false)
	)



val dataframe_with_schema = spark.read.format("csv")
	.option("header", "false")
	.schema(schema)
	.load("<data_file_path>")




// Read CSV files with a user-specified schema
// When the schema of the CSV file is known upfront, you can specify the desired schema to the CSV reader with the schema option.


val schema = new StructType()
	.add("_c0",  	IntegerType,true)
	.add("carat",	DoubleType,	true)
	.add("cut",		StringType,	true)
	.add("color",	StringType,	true)
	.add("clarity",	StringType,	true)
	.add("depth",	DoubleType,	true)
	.add("table",	DoubleType,	true)
	.add("price",	IntegerType,true)
	.add("x",		DoubleType,	true)
	.add("y",		DoubleType,	true)
	.add("z",		DoubleType,	true)


val dataframe_with_schema = spark.read.format("csv")
  .option("header", "true")
  .schema(schema)
  .load("<data_file_path>")





3. Verify correctness of the DataFrame

// When reading CSV files with a user-specified schema, it is possible that the actual data in the files does not match the specified schema. 
// For example, a field containing name of the city will not parse as an integer. The consequences depend on the mode that the parser runs in:

	PERMISSIVE (default): nulls are inserted for fields that could not be parsed correctly
	DROPMALFORMED: drops lines that contain fields that could not be parsed
	FAILFAST: aborts the reading if any malformed data is found

val dataframe_with_schema = spark.read.format("csv").option("mode", "PERMISSIVE")

// In the PERMISSIVE mode it is possible to inspect the rows that could not be parsed correctly. To do that, you can add _corrupt_record column to 
// the schema.


// PERMISSIVE Mode
val schema = new StructType()
	.add("_c0",		IntegerType,true)
	.add("carat",	DoubleType,	true)
	.add("cut",		StringType,	true)
	.add("color",	StringType,	true)
	.add("clarity",	StringType,	true)
	.add("depth",	IntegerType,true) // The depth field is defined wrongly. The actual data contains floating point numbers, while the schema specifies an integer.
	.add("table",	DoubleType,	true)
	.add("price",	IntegerType,true)
	.add("x",		DoubleType,	true)
	.add("y",		DoubleType,	true)
	.add("z",		DoubleType,	true)
	.add("_corrupt_record", StringType, true) 
// The schema contains a special column _corrupt_record, which does not exist in the data. This column captures rows that did not parse correctly.

val dataframe_with_wrong_schema = spark.read.format("csv")
  .option("header", "true")
  .schema(schema)
  .load("<data_file_path>")



// The mistake in the user-specified schema causes any row with a non-integer value in the depth column to be nullified.
// There are some rows, where the value of depth is an integer e.g. 64.0. They are parsed and coverted successfully.
// The _currupt_record column shows the string with original row data, which helps find the issue.
// For rows that parsed correctly, 
display(diamonds_with_wrong_schema)  



// DROPMALFORMED mode
val diamonds_with_wrong_schema_drop_malformed = spark.read.format("csv")
	.option("mode", "DROPMALFORMED")
	.option("header", "true")
	.schema(schema)
	.load("<data_file_path>")


// FAILFAST mode
val diamonds_with_wrong_schema_fail_fast = sqlContext.read.format("csv")
  .option("mode", "FAILFAST")
  .option("header", "true")
  .schema(schema)
  .load("<data_file_path>")



_____________________________________________________________________________________________________________________________________________________


// Available Option whil reading a file in DataFrame

https://github.com/apache/spark/blob/branch-2.1/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/csv/CSVOptions.scala


// DataFrame .options()
	.option("charset", "utf-8")
	.option("header", "true")
	.option("quote", "\"")~
	.option("delimiter", ",")




	.format("com.databricks.spark.csv")



	