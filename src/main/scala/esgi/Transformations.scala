package esgi

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, to_date, window}
import org.apache.spark.sql.streaming.StreamingQuery

case class Transformations(stream: DataFrame){

  def totalActionsOnSite(): Unit ={
    val streamCase = stream.selectExpr("avg_time_on_site", "nb_actions", "date", "nb_actions_per_visit")
      .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

    streamCase.writeStream.format("memory").queryName("totalActionsOnSite").outputMode("append").start()
  }

  def average_by_window(): StreamingQuery = {
    val avg_of_avg = stream
      .selectExpr(
        "avg_time_on_site/60 as avg_time_on_site",
        "avg_time_on_site as avg_time_on_site_seconds",
        "date",
      )
      .groupBy(
        window(col("date"), "10 days")
          .as("window"))
      .avg("avg_time_on_site_seconds")
      .orderBy("avg(avg_time_on_site_seconds)")
    avg_of_avg.writeStream.format("memory").queryName("avg_time").outputMode("complete").start()

  }

}
