import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

// To decrease the logging level of Spark
import org.apache.log4j.Logger
import org.apache.log4j.Level

object SparkRDD01 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SparkRDD01")

    val sc = new SparkContext(conf)

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val violations_rdd = sc.textFile("file:///D:/NotebookShare/Material/Hadoop/DataSet/violations_plus.csv")

    println(violations_rdd)
    //

    println("Count: ", violations_rdd.count)
    // MapPartitionsRDD[1]  -- What does it mean??


    println("id: ", violations_rdd.id)
    println("name: ", violations_rdd.name)
    println("first: ", violations_rdd.first())

    println("Partition Size: ", violations_rdd.partitions.size)
    // Partition size -> 2


    // Explictly specifying the number of partitions
    val voilations_rdd2 = sc.textFile("file:///D:/NotebookShare/Material/Hadoop/DataSet/violations_plus.csv", 5)
    println("Partition Size: ", voilations_rdd2.partitions.size)
    // Partition size -> 5


  }
}
