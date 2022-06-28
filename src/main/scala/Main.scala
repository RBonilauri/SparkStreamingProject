import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

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

    val arrayConfig = spark.sparkContext.getConf.getAll

    println("= SPARK CONFIG =")
    for (conf <- arrayConfig) {
      println(s"\t${conf._1} : ${conf._2}")
    }

  }
}