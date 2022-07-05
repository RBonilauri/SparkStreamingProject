package esgi

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File
import java.nio.file.{Files, StandardCopyOption}

object Main {
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  val dataDir: String = String.format( "%s/%s/",System.getProperty("user.dir") , "data")

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

  def getDefaultSchema(sparkSession: SparkSession): StructType = {

    val data_importer = new DataImporter()
    val df: DataFrame = data_importer.readCSV(sparkSession, getListOfFiles(dataDir).head)

    df.schema
  }

  def controlStop(spark: SparkSession): StreamingQuery = {
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

  def resetDir(): Unit = {

    val d1 = new File(dataDir + "stop_stream/stop_stream.csv").toPath
    val d2 = new File(dataDir + "tmp/stop_stream.csv").toPath

    Files.move(d1, d2, StandardCopyOption.ATOMIC_MOVE)
  }

  def main(args: Array[String]): Unit = {

    val spark = initSparkSession()

    val streamSchema = getDefaultSchema(spark)

    val siteUseStream = spark.readStream
      .schema(streamSchema)
      .option("maxFilesPerTrigger", 1)
      .format("csv")
      .option("header", "true")
      .load(dataDir + "*.csv")

    val stopStream = controlStop(spark)


    println("retail is streaming: " + siteUseStream.isStreaming)
    println("supervisor is streaming : " + stopStream.isActive)


    Transformations(siteUseStream).launchStreams()

    var stopStatus = true
    while( stopStatus ) {
      val totalActions = spark.sql(
        """
          |select max(date) as MAX_date, sum(nb_actions) as TOTAL_actions  from totalActionsOnSite
          |""".stripMargin)
      println("total action on site up to date : " )
      totalActions.show(truncate = false)

      val dfSelect = spark.sql(
        """
          |select * from avg_time
          |order by window desc
          |""".stripMargin)

      dfSelect.show(numRows = 10, truncate = false)

      val df = spark.sql(
        "select * from stop_process".stripMargin)
      stopStatus = df
        .count() == 0
      df.show()

      val dfBusy = spark.sql("select * from big_busiest_day")
      dfBusy.show()
      Thread.sleep(5000)
    }
    spark.stop()

    resetDir()
  }
}
