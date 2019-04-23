import org. apache. spark.sql.functions.desc
import org. apache. spark.sql.functions.asc





DataFrame.orderBy("<column_name>").show()
DataFrame.orderBy(asc("<column_name>")).show()
DataFrame.orderBy(desc("<column_name>")).show()

DataFrame.orderBy($"<column_name>".desc).show()

DataFrame.orderBy(desc("<column_name1>"), desc("<column_name2>"), asc("<column_name3>"))

-- DataFrame.orderBy returns a DataSet[Row]





DataFrame.sort("<column_name>").show()
DataFrame.sort(desc("<column_name>")).show()

DataFrame.sort($"<column_name1>", $"<column_name2>" desc)



