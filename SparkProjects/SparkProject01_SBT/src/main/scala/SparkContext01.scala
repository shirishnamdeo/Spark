// Before Spark 2.0 entry point (transfer of control from OS to the program) is the Spark Context
// RDD was the main API, and it was created and manipulated by Spark Context
// For every other API we need to have separate context
// For Streaming, streamingContext, SQL sqlContext, Hive hiveContext

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object SparkContext01 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SparkContext01").setMaster("local[*]")
    // "local", -> To run locally
    // local[n] -> TO run locally with n cores

    val sc = new SparkContext(conf)
    println(sc.version)
  }
}
