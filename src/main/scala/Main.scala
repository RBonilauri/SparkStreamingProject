import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, to_date, window}
import org.apache.spark.sql.{DataFrame, SparkSession}

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

    /*val arrayConfig = spark.sparkContext.getConf.getAll
    println("= SPARK CONFIG =")
    for (conf <- arrayConfig) {
      println(s"\t${conf._1} : ${conf._2}")
    }*/

    val data_dir =  System.getProperty("user.dir") + "\\data\\"
    val data_importer = new DataImporter()

    var BigDF : DataFrame = spark.emptyDataFrame

    getListOfFiles(data_dir).foreach(file =>
      BigDF = data_importer.unionCSV(BigDF, data_importer.readCSV(spark,file))
    )
    println("All files loaded")
    val siteUseSchema = BigDF.schema

    val siteUseStream = spark.readStream
      .schema(siteUseSchema)
      .option("maxFilesPerTrigger", 1)
      .format("csv")
      .option("header", "true")
      .load(data_dir + "*.csv")

    siteUseStream.createOrReplaceTempView("time_on_data_dot_gouv")

    //      .writeStream
//      .queryName("Writer")
//      .foreachBatch((batchDf : DataFrame , batchId : Long) => {
//        print("looping")
//        batchDf.show()
//        if (batchDf.count() >= 100) needsToStop = true
//      })
//      .start()

    println("retail is streaming: " + siteUseStream.isStreaming)
//    while(!needsToStop) {
//      if(needsToStop) {
//        retailStream.stop()
//      }
//      Thread.sleep(1000)
//    }
    val test = siteUseStream.selectExpr("avg_time_on_site" , "nb_actions",  "date")
      .withColumn("date", to_date(col("date"),"yyyy-MM-dd"))

//  .groupBy(col("nb_actions_per_visit")).sum()
    test.writeStream.format("memory").queryName("time_on_site").outputMode("append").start()

    val avg_of_avg = siteUseStream
      .selectExpr(
        "avg_time_on_site",
        "date",
      )
      .groupBy(
        window(col("date"), "10 days").as("window")
      )
      .avg("avg_time_on_site")

    avg_of_avg.writeStream.format("memory").queryName("avg_time").outputMode("complete").start()


    for (_ <- 1 to 100) {
      println("data in time_on_site: " + spark.sql(
        """
          |select * from time_on_site
          |""".stripMargin)
        .count())

      spark.sql("""
        |select * from avg_time
        |order by window
        |""".stripMargin).show(100, truncate = false)
      Thread.sleep(5000)
    }
    System.in.read()
    spark.stop()
  }
}