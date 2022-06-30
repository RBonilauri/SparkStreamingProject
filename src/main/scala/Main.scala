import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.io.File

object Main {

  def getListOfFiles(dir: String):List[String] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList.map(_.toString)
    } else {
      List[String]()
    }
  }
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

    val data_dir =  System.getProperty("user.dir") + "\\data"
    val data_importer = new DataImporter()
    val list_of_files = getListOfFiles(data_dir)
    var BigDF : Dataset[Row] = data_importer.readCSV(spark, list_of_files.head)

//    list_of_files.foreach(_ => {
//      BigDF = data_importer.unionCSV(BigDF, data_importer.readCSV(spark,_))
//    })

//    BigDF = data_importer.unionCSV(BigDF,BigDF)
    list_of_files.foreach(
        _ : String =>{ BigDF  = data_importer
          .unionCSV(BigDF, data_importer.readCSV(spark, _))
        })
      )
  }
}