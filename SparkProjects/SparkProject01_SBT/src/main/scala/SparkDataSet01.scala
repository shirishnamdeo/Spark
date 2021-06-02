import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.Dataset

import org.apache.log4j.Logger
import org.apache.log4j.Level

object SparkDataSet01 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkDataSet01")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val rootLogget = Logger.getRootLogger()
    rootLogget.setLevel(Level.ERROR)

    // To load the data inta a DataSet, the datastructure of the data needs to be defined.
    // We are using the case class to define the struct



    case class Voilation_Class(business_id: Int, date: String, ViolationTypeID: String, risk_category: String, description: String)
    // val voilationd_ds = spark.read.option("header", "true").csv("file:///D:/NotebookShare/Material/Hadoop/DataSet/violations_plus.csv")


    // Another way of defining the schema
    // import org.apache.spark.sql.types.StructType
    // val schema = new StructType()
    //  .add($"id".long.copy(nullable = false))
    //  .add($"city".string)
    //  .add($"country".string)

  }
}
