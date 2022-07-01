import org.apache.spark
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class DataImporter {

  def readCSV(sparkSession: SparkSession, fileName: String): DataFrame = {

    val csvToRead: DataFrame = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(fileName)

    csvToRead
  }

  def unionCSV(allData: DataFrame, newData: DataFrame): DataFrame = {
    val newUnion: DataFrame = allData
      .unionByName(newData, allowMissingColumns = true)

    newUnion
  }
}
