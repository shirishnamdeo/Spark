// __________________________________________________________________________________________________________________________________________________

// Method to take a dataFrame and Array of String with column names to be excluded, and return the dataFrame with excluded columns.

def Method_ExcludeColumnsFromDF(dataFrame: DataFrame, excluded_column_list: Array[String]) = {
    val included_column_list = dataFrame.columns.filter( column_name => !excluded_column_list.contains(column_name))
    dataFrame.select(included_column_list.head, included_column_list.tail: _ *)
}

 

// val excluded_column_list = Array("ingestion_ts", "desrc")
// Method_ExcludeColumnsFromDF(DataFrame, excluded_column_list)


// __________________________________________________________________________________________________________________________________________________


// Takes a Prediction dataFrame, extract the labels and prediction columns from it, and return the PAIR rdd with the two values as a pair.

def Method_LabelPrediction_RDD(prediction_df: DataFrame) = {
    val label_prediction_rdd = prediction_df.select("label", "prediction").rdd
    val prediction_label_rdd = label_prediction_rdd.map(x => (x(1).toString.toDouble, x(0).toString.toDouble))
    prediction_label_rdd
}

// __________________________________________________________________________________________________________________________________________________


 

// Method to take a dataframe and a column to group on, and provide the statistics on its count, count percentage, cummulative count and 
// cumm_percentage

import org.apache.spark.sql.functions._
import org. apache. spark.sql.expressions.Window

def Method_GrpCount(dataFrame: DataFrame, column_name: String) = {
    val count_column = "count_" + column_name
    val countDF = dataFrame.select(col(column_name)).groupBy(column_name).agg(count(column_name).alias(count_column)).withColumn("count_percentage", format_number(col(count_column) / dataFrame.count, 8)).orderBy(desc(count_column))
    val countDF_cummulative = countDF.withColumn("cummulative_sum", sum(count_column).over(Window.orderBy(desc(count_column)))).withColumn("cumm_percentage", format_number($"cummulative_sum" / dataFrame.count, 8))
    countDF_cummulative
}




// Below DistinctCount function will just count the distinct values in each group. Method GrpCount is more informative.
def Method_DistinctCount(dataFrame: DataFrame, column_name: String) = {
    val column_name_count = column_name + "_count"
    dataFrame.select(col(column_name)).groupBy(column_name).agg(count(column_name).alias(column_name_count)).orderBy(desc(column_name_count))
}

// Method_GrpCount(dataFrame3, "<column_name>").show(50)


// __________________________________________________________________________________________________________________________________________________


// Spake Cross Tab with count and ordered

// Efficient Code
def Method_Crosstab_With_Count(dataFrame: DataFrame, left_column_name: String, upper_column_name: String) = {
    val crosstabDF = dataFrame.stat.crosstab(left_column_name, upper_column_name)
    //
    // Excluding the first column name, and incuding all the column representing the values of a upper_column_name's column.
    val columnsToSum = crosstabDF.columns.filter(!_.contains(crosstabDF.columns.head))
    val crosstabDF_with_HorizontalSum = crosstabDF.withColumn("horizontalSum", columnsToSum.map(x => col(x)).reduce((c1, c2) => c1 + c2)).orderBy(desc("horizontalSum"))
    crosstabDF_with_HorizontalSum
}

 
// Uneffieffieient Method involving join. 
// def Method_Crosstab_With_Count(dataFrame: DataFrame, left_column_name: String, upper_column_name: String) = {
//  val crosstabDF = dataFrame.stat.crosstab(left_column_name, upper_column_name)
//  val countDF = dataFrame.select(col(1eft_column_name)).groupBy(1eft_column_name).agg(count(left_column_name))
//  val joinedDF = crosstabDF.join(countDF, col("human_tax_cat_business_unit"), col("human_tax_cat"), "inner").orderBy(desc("count(human_tax_cat)"))
//  joinedDF
// }



// val left_column_name = "human_tax_cat"
// val upper_column_name = "business_unit"

// Method_Crosstab_With_Count(dataFrame3, left_column_name, upper_column_name).show(50)
// Method_Crosstab_With_Count(dataFrame3, "<column_name1>", "<column_name2>").show(50)


[https://stackoverflow.com/questions/37624699/adding-a-column-of-rowsums-across-a-list-of-columns-in-spark-dataframe/37625225]
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{lit, col}

def sum_(cols: Column*) = cols.foldLeft(lit(0))(_ + _)

val columnstosum = Seq("var1", "var2", "var3", "var4", "var5").map(col _)
df.select(sum_(columnstosum: _*))

// __________________________________________________________________________________________________________________________________________________


