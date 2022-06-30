import org.apache.spark
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class DataImporter {

  def readCSV(sparkSession: SparkSession, fileName: String): Dataset[Row] = {

    val csvToRead: Dataset[Row] = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(fileName)

    csvToRead
  }

  def unionCSV(allData: Dataset[Row], newData: Dataset[Row]): Dataset[Row] = {

    val newUnion: Dataset[Row] = allData
      .unionByName(newData, allowMissingColumns = true)

    newUnion
  }
}
