import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.log4j.Logger
import org.apache.log4j.Level

object SparkRDDTextFile01 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SparkRDDTextFile01")

    val sc = new SparkContext(conf)

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    // Reading From Local
    val violation_rdd = sc.textFile("file:///D:/NotebookShare/Material/Hadoop/DataSet/violations_plus.csv")

    println(violation_rdd.map(l => l.length()).reduce((a,b) => a+b))
    // 3799557 -> Conut of all the characters in file

    println(violation_rdd.map(l => 1).reduce((a,b) => a+b))
    // 46908 -> I believe these are the number of lines

    println(violation_rdd.count())
    // 46908
    // So each line in the file/RDD is one entity for operations (by Default and also in MAP).
    // textFile -> Return one records each line of each file
  }
}
