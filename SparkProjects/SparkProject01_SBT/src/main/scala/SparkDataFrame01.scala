import org.apache.spark.sql.SparkSession

import org.apache.log4j.Logger
import org.apache.log4j.Level

object SparkDataFrame01 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkDataFrame01")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val rootLogget = Logger.getRootLogger()
    rootLogget.setLevel(Level.ERROR)

    val voilationd_df = spark.read.csv("file:///D:/NotebookShare/Material/Hadoop/DataSet/violations_plus.csv")

    println(voilationd_df)
    println("Count: ", voilationd_df.count())

    println(voilationd_df.columns)
    println(voilationd_df.printSchema())
    println(voilationd_df.select("_c1").show())


    // Reading header of the file implicitly.
    val voilationd_df2 = spark.read.option("header", "true").csv("file:///D:/NotebookShare/Material/Hadoop/DataSet/violations_plus.csv")
    println(voilationd_df2.columns)
    println(voilationd_df2.printSchema())
    println(voilationd_df2.select("business_id").show())


  }
}
