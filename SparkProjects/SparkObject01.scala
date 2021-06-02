package saprkpackage01

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkObject01 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("Spark App 01")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile( path = "file:///D:/SoftwareInstalled/Spark/Spark240/spark-2.4.0-bin-hadoop2.7/README.md")
    val tokenizedFileData = textFile.flatMap(line=>line.split(" "))
    val countPrep = tokenizedFileData.map(word=>(word, 1))
    val counts = countPrep.reduceByKey((accumValue, newValue) => accumValue + newValue)
    val sortedCounts = counts.sortBy(kvPair => kvPair._2, ascending = false)
    sortedCounts.saveAsTextFile( path = "file:///D:/NotebookShare/Material/Hadoop/ApacheSpark/output_data/sorted_workcounts_exmaple")

  }
}
