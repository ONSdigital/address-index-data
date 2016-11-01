package uk.gov.ons.addressindex.readers

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import uk.gov.ons.addressindex.models.CSVSchemas
import uk.gov.ons.addressindex.utils.SparkProvider

/**
  * Contains static methods to read different cvs files related to the Address Index
  */
object AddressIndexFileReader {

  private lazy val config = ConfigFactory.load()
  private lazy val pathToCsv = config.getString("addressindex.folders.csv")

  /**
    * Reads csv into a `DataFrame`
    *
    * @param fileName name of the CSV file
    * @return `DataFrame` containing the delivery point data from CSV
    */
  def readDeliveryPointCSV(fileName: String): DataFrame =
    SparkProvider.sqlContext.read
      .format("com.databricks.spark.csv")
      .schema(CSVSchemas.deliveryPointSchema)
      .option("header", "true") // Use first line of all files as header
      .load(s"$pathToCsv/$fileName")

}
