// Method to take a dataFrame and Array of String with column names to be excluded, and return the dataFrame with excluded columns.

def Method_ExcludeColumnsFromDF(dataFrame: DataFrame, excluded_column_list: Array[String]) = {
    val included_column_list = dateFrame.columns.filter( column_name => !excluded_column_list.contains(column_name))
    dataFrame.select(included_column_list.head, included_column_list.tail: _ *)
}
  

// val excluded_column_list = Array("ingestion_ts", “desrc”)
// Method_ExcludeColumnsFromDF(DataFrame, excluded_column_list)



 

// Takes a Prediction DateFrame, extract the labels and prediction columns from it, and return the rdd with the two values as a pair.

def Method_LabelPrediction_RDD(prediction_df: DataFrame) = {
    val label_prediction_rdd = prediction_df.select("label", “prediction”).rdd
    val prediction_label_rdd = label_prediction_rdd.map(x => (x(1).toString.toDouble, x(0).toString.toDouble))
    prediction_label_rdd
}


def Method_LabelPrediction(prediction_df: DataFrame) = {
    val label_prediction = prediction_df.select($"label", $"prediction")
    val label_prediction2 = label_prediction.withColumn("flag", when($"label" === $"prediction", 1) .otherwise(0))
    label_prediction2.groupBy("label").agg(count("label") as "category_count", sum("flag") as "correct_prediction").withColumn("diff", $"category_count" - $"correct_prediction").withColumn("%diff", $"correct_prediction" / $"category_count")
}




// Method to take a dataframe and a column to group on, and provide the statistics on its count, count percentage, cummulativee count and cumm_percentage

import org.apache.spark.sql.functions._
import org. apache. spark.sql.expressions Window

def Method_GrpCount(dataFrame: DataFrame, column_name: String) = {
    val count_column = "count_" + column_name
    val countDF = dataFrame.select(col(column_name)).groupBy(column_name).agg(count(column_name).alias(count_column)).withColumn("count_perentage", format_number(col(count_column) / dataFrame.count, 8)).orderBy(desc(count_column) )
    val countDF_cummulative = countDF.withColumn("cummulative_sum", sum(count_column).over(Window.orderBy(desc(count_column)))).withColumn("cumm_percentage", format_number($"cummulative_sum" /dataFrame.count, 8))
	countDF_cummulative
}


// Method_GrpCount is having a bug, when morethan one value is same, it calculates the group sum for each element.
// Need to fix this

//|voucher_id| count_voucher_id| count_perentage| cummulative_sum|cumm_percentage|
//| 0051543) 20| 0.00087516| 100| 0.00437579]|
//| 0051542) 20| @.00087516| 100 0.00437579|
//| 0023714) 20| @.00087516| 100 0.00437579|
//| 08051416) 20| @.00087516| 100 0.00437579|
//| 00051564) 20| 0.00087516| 100| 0.00437579|
//| 0011749) 18| 0.00078764| 118| 0.00516344|
//| 0047327) 17| 0.00074388| 135] 8.00590732|
//| 0003338) 16| 8.00070013| 151| 0.00660745|
//| 0043448) 12| 8.00052510| 163| 0.00713254|
//| 08011194] 11| 0.00048134| 174| 0.00761388|

 

________________________


def Method_MedianFraction(dataFrame: DataFrame, column_name: String) = {
	val quantileProbability = 0.5
	val count_column = "count_" + column_name
	val countDF = dataFrame. select(col(column_name)).groupBy(column_name).agg(count(column_name).alias(count_column)).withColumn("count_perentage", format_number(col(count_column) / dataFrame.count, 8)).orderBy(desc(count_column) )
	val countDF_cummulative = countDF.withColumn("cummulative_sum", sum(count_column) .over(Window.orderBy(desc(count_column)))) .withColumn("cumm_percentage", format_number($"cummulative_sum" /dataFrame.count, 8))
	//
	val medianValueHTCcount = countDF_cummulative.stat.approxQuantile(col=count_column, probabilities=Array(quantileProbability), relativeError=0.001)(0).toDouble
    val qunatileFractionDF = countDF_cummulative.withColumn("toLableFraction", (lit(medianValueHTCcount)/$"count_human_tax_cat").cast("decimal(10, 4)"))
    val samplingLevel = qunatileFractionDF.withColumn("samplingLevel", $"toLableFraction"< 1)
    //
	// Later we can even introduce the sampling level, so that samples with very high data representation do not get extensively undersampled, and data with too less representation, do not get to much over sampled.
    samplingLevel
}

// tolableFraction after formating (using format_number) give a comma separated values for large numbers, which is not treated as double. So be cautious to not to use the result directly.
// df.select(col("toLableFraction").cast("decimal(10,4)")).show(50)
// Problem solved with .cast(decimal)

  

 

def divideFunction( val_1: Double, val_2: Double ) : Double = {
    return val_1/val_2
}

val divideUDF = udf(divideFunction(_:Double,_:Double))

// Belwo DistinctCount function will just count the distinct values in each group. Method GrpCount is more informative.
def Method_DistinctCount(dateFrame: DataFrame, column_name: String) = {
    val column_name_count = column_name + "_count"
    dataFrame.select(col(column_name)).groupBy(column_name).agg(count(column_name) alias (column_name_count)) .orderBy(desc(column_name_count))
}

// Method_GrpCount(dateFrame3, "human_tax_cat"). show(50)
// Method _MedianFraction(dataFrame3, "human_tax_cat").show(5@)




_____________________________




// Spake Cross Tab with count and ordered

// Efficient Code
def Method_Crosstab_With_Count(dataFrame: DataFrame, left_column_name: String, upper_column_name: String) = {
	val crosstabDF = dataFrame.stat.crosstab(left_column_name, upper_column_name)
	val columnsToSum = crosstabDF.columns.filter(!_.contains(crosstabDF.columns.head))
	val crosstabDF_with_HorizontalSum = crosstabDF.withColumn("horizontalSum", columnsToSum.map(x => col(x)).reduce((c1, c2) => cl + c2)).orderBy(desc("horizontalSum"))
	crosstabDF_with_HorizontalSum
}

 
def Method_Crosstab_With_Count2(dataFrame: DataFrame, left_column_name: String, upper_column_name1: String, upper_column_name2: String) = {
	val crosstabDF = dataFrame.stat.crosstab(left_column_name, upper_column_name1)
	val crosstabDF2 = dataFrame.stat.crosstab(left_column_name, upper_column_name2)
	val joinedDF = crosstabDF.join(crosstabDF2, col(left_column_name ++ "_" ++ upper_column_name1) === col(left_column_name ++ "_" ++ upper_column_name2))
	joinedDF
}
 

def Method_Crosstab_With_Count3(dataFrame: DataFrame, left_column_name: String, upper_column_namel: String, upper_column_name2: String, upper_column_name3: String) = {
	val crosstabDF = dataFrame.stat.crosstab(left_column_name, upper_column_name1)
	val crosstabDF2 = dataFrame.stat.crosstab(left_column_name, upper_column_name2)
	val crosstabDF3 = dataFrame.stat.crosstab(left_column_name, upper_column_name3)
	val joinedDF = crosstabDF.join(crosstabDF2, col(left_column_name ++ "_" ++ upper_column_name1) === col(left_column_name ++ "_" ++ upper_column_name2))
	val joinedDF2 = joinedDF.join(crosstabDF3,  col(left_column_name ++ "_" ++ upper_column_name1) === col(left_column_name ++ "_" ++ upper_column_name3))
	joinedDF2
}	


// scala> Method_Crosstab_With_Count(dataFrame3, "tax_cd_vat", "tax_cd_vat_pct").show(50)
// ERROR

// Uneffieffieient Method
// def Method_Crosstab_With_Count(dataFrame: DataFrame, left_column_name: String, upper_column_name: String) = {
// val crosstabDF = dataFrame.stat.crosstab(left_column_name, upper_column_name)
// val countDF = dataFrame.select(col(1eft_column_name)).groupBy(1eft_column_name) .agg(count(left_column_name))
// val joinedDF = crosstabDF.join(countDF, col("human_tax_cat_business_unit") === col ("human_tex_cat"), "inner") .orderBy(desc(“count(human_tax_cat)"))
// joinedDF
//}


_____________________



// val left_column_name = "human_tax_cat"
// val upper_column_name = "business_unit"
// Method_Crosstab_With_Count(dataFrame3, left_column_name, upper_column_name).show(50)
// Method_Crosstab_With_Count(dataFrame3, "human_tax_cat", "business_unit").show(50)

 
 
// val left_column_name = "human_tax_cat"
// val upper_column_namel = "vat_rgstrn_country"
// val upper_column_name2 = "posting process"
// Method_Crosstab_With_Count2(dataFrame3, "human_tex_cat", "vat_rgstrn_country", "posting process")
// Method_Crosstab_With_Count3(dataFrame3, "human_tex_cat", "vat_rgstrn_country", "posting process", "currency_cd")


//val dataFreme = dataFrame3
// val column_name = "human_tax_cat"

// https: //stackoverflow.com/questions/49252670/iterate-rows-and-columns-in-spark-dataframe [IMP]



_____________________



scala> List("aa", "bbb", "bb", "bbb").groupBy(identity)
res91: scala.collection. immutable.Map[String,List[String]] = Map(bbb -> List(bbb, bbb), bb -> List(bb), aa -> List(aa))

-- groupBy(identity) work in List above but not in below

scala> dataFrame3.select($"vendor_id", $"vendor_name1") . groupBy(identity)

_____________________________________


import org.apache. spark.ml.stat.ChiSquareTest
import org.apache.spark.ml.stat.Correlation

// In Spark to perform the test of independence you need to convert Dataframe(the crosstab dataframe / contingency table itself) to a Matrix.
Convert Dataframe to RDD (drop the first column Diet_Outcome)
Obtain the values from RDD
3. Obtain the row and column count from contingency table
Using the above obtain dense matrix
Run Chi Square test of independence on the matrix


val crosstab_BU_BUGL = Method_Crosstab_With_Count(dataFrame3, "business_unit", "business_unit_gl")
val contingency_df=crosstab_BU_BUGL.drop("business_unit_business_unit_gl", "horizontalSum")
val contingency_rdd=contingency_df.as[ (Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double) ].rdd
val values = contingency_rdd.map(row => List(row._1,row._2,row._3,row._4,row._5,row._6,row._7,row._8,row._9,row._10,row._11,row._12)).flatMap(row => row)

val row_count = contingency_df.count().toInt
val column_count = contingency_df.columns.size

import org.apache.spark.mllib.stat.Statistics
import org.apache. spark.mllib.stat.test.ChiSqTestResult
import org.apache.spark.mllib.linalg.__

val mat: Matrix = Matrices.dense(row_count, column_count, values)
val independenceTestResult = Statistics. chiSqTest(mat)
-- Somehow a zero value is comming into a row, so can’t able to find the output.Solution, column_count will combe before row_count



If the p-value is low, we can reject the null hypothesis and say that there is a dependency between diet and health
outcome.

NULL Hypothesis: The null hypothesis for a chi-square independence test is that two categorical variables are
independent in some population.







// https: //stackoverflow. com/questions/51184319/convert-dataframe-into-spark-mllib-matrix-in-scala

// filter out the columns which are needed in matrix
val matrixColumns = contingency_df.columns.filter(!_.contains("business_unit_business_unit_gl")).filter(!_.contains("horizontalSum")).map(col(_))
val arr = contingency_df.select(array(matrixColumns:_*).as("arr")).as[Array[Double]].collect().flatten.map(_.toDouble)

 
val rows = contingency_d¥.count().toInt,
val cols = matrixColumns. length

// It's necessary to reverse cols and rows here and then transpose
val dm = Matrices.dense(cols, rows, arr).transpose

scala> println(Matrices.dense(cols, rows, arr).toString(24, Int.MaxValue))




scala> val independenceTestResult = Statistics. chiSqTest(dm)
independenceTestResult: org.apache. spark.mllib.stat.test.ChiSqTestResult = Chi squared test summary:
method: pearson
degrees of freedom = 253 <= 11*23
statistic - 2.122027405828809E7
pValue = 0.0
Very strong presumption against null hypothesis: the occurrence of the outcomes is statistically independent.
-- Thus there is high depe

https: //stackoverflow.com/questions/51184319/convert-dataframe-into-spark-mllib-matrix-in-scala

scala> contingency_df.select(array(matrixColumns:_*).as(“arr”)) .show(50)
scala> contingency_df.select(array(matrixColumns:_*).as[Array[Double]].show(50)

 


_______________________


// Method to find out the ChiSquare value between two categorical variables.
def Method_ChiSquareTest(dataFrame: DataFrame, categorical_col1: String, categorical_col2: String) = {
	//
	import org.apache.spark.mllib.stat.Statistics
	import org.apache.spark.mllib.stat.test.ChiSqTestResult
	import org.apache.spark.mllib.linalg.__
	//
	val crosstabDF = dataFrame.stat.crosstab(categorical_coll, categorical_col2)
	val matrixColumns = crosstabDF.columns.filter(!_.contains(crosstabDF.columns.head)).map(col(_))
	val value_array = crosstabDF.select(array(matrixColumns:_*).as("arr")).as[Array[Double]].collect().flatten.map(_.toDouble)
	val rows = crosstabDF.count.toInt
	val cols = matrixColumns.length
	//
	val dm = Matrices.dense(cols, rows, value_array).transpose
	//# printin(Matrices.dense(cols, rows, value_array).toString(24, Int.MaxVelue))
	//# printin(dm.toString(24, Int.MaxValue))
	//
	val independenceTestResult = Statistics.chiSqTest(dm)
	independenceTestResult
}



import org.apache.spark.mllib.stat.Statistics

def Method_Correlation(dataFrame: DataFrame, column_namel: String, column_name2: String) = {
	//
	import org.apache.spark.mllib.stat.Statistics
	val x = dataFreme3.select($"monetary_amount").map(row => row(0).toString.toDouble)
	val y = dataFrame3.select ($"foreign_amount").map(row => row(@). toString. toDouble
	//
	Statistics.corr(x.rdd, y.rdd, method="pearson")
}

scala> Statistics.corr(x.rdd, y.rdd)
res27: Double = 0.9702229840602735

 

import org.apache. spark.mllib. linalg.Vector
Spark Vectors are not distributed, therefore are applicable only if data fits in memory of one (driver) node. -- Is
it? |

 

// https: /github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/ml/stat/Correlation. scala
// How to convert Spark DataFrame column into a Vecotr?
// How to convert Spark DataFrame into a Dataset[Vector]?


