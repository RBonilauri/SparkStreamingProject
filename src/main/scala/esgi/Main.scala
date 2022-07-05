package esgi

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, sum, to_date, window}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File

object Main {
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  def getListOfFiles(dir: String): List[String] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList.map(_.toString)
    } else {
      List[String]()
    }
  }


  def initSparkSession(): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName("SparkStreamingProject")
      .master(master = "local[*]")
      .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", "5")
    spark
  }

  def getDefaultSchema(sparkSession: SparkSession, directory : String): StructType = {

    val data_importer = new DataImporter()
    val df: DataFrame = data_importer.readCSV(sparkSession, getListOfFiles(directory).head)

    df.schema
  }

  def controlStop(spark: SparkSession, dataDir: String): StreamingQuery = {
    val stopSchema = "process string, kill string"
    val stopStream = spark.readStream
      .schema(stopSchema)
      .format("csv")
      .option("header", "true")
      .load(dataDir + "stop_stream/*")

    stopStream
      .select("process", "kill")
      .writeStream
      .format("memory")
      .queryName("stop_process")
      .outputMode("append")
      .start()
  }

  def main(args: Array[String]): Unit = {

    val spark = initSparkSession()

    val dataDir = String.format( "%s/%s/",System.getProperty("user.dir") , "data")
    val streamSchema = getDefaultSchema(spark, dataDir)

    val siteUseStream = spark.readStream
      .schema(streamSchema)
      .option("maxFilesPerTrigger", 1)
      .format("csv")
      .option("header", "true")
      .load(dataDir + "*.csv")

    val stopStream = controlStop(spark, dataDir)


    println("retail is streaming: " + siteUseStream.isStreaming)
    println("supervisor is streaming : " + stopStream.isActive)


    val transformations = Transformations(siteUseStream)

    transformations.totalActionsOnSite()
    transformations.average_by_window()

    var stopStatus = true
    while( stopStatus ) {
      val totalActions = spark.sql(
        """
          |select max(date) as MAX_date, sum(nb_actions) as TOTAL_actions  from totalActionsOnSite
          |""".stripMargin)
      println("total action on site up to date : " )
      totalActions.show(totalActions.count().toInt ,truncate = false)

      val dfSelect = spark.sql(
        """
          |select * from avg_time
          |order by window
          |""".stripMargin)

      dfSelect.show(numRows = 5, truncate = false)

      val df = spark.sql(
        "select * from stop_process".stripMargin)
      stopStatus = df
        .count() == 0
      println()
      df.show()
      Thread.sleep(5000)
    }
    spark.stop()
  }
}
