1. Creating DataFrame from Hive Tables

// Show all the databases.
spark.sql("SHOW DATABASES")
// Right now it shows all the databases and 

spark.sql("SHOW DATABASES LIKE '<db_name>*' ").show()




// Describe Table (both variant works!!)
spark.sql("DESCRIBE <db-namespace>.<table-name>")
spark.sql("DESCRIBE TABLE <db-namespace>.<table-name>")
spark.sql("DESCRIBE FORMATTED <db-namespace>.<table-name>")


// But for database it doesn't work 
spark.sql("DESCRIBE <db-namespace>")
spark.sql("DESCRIBE DATABASE <db-namespace>")





// Loading data from hive-table
spark.sql("SELECT * FROM <db-namespace>.<table-name>")


