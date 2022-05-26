import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.Console.println

object Main {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkStreamingProject")
      .master(master = "local[*]")
      .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", "5")

  }
}
