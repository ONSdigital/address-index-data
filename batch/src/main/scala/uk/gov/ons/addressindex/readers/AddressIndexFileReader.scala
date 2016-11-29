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
  private lazy val pathToCsv = config.getString("addressindex.files.csv.delivery-point")
  private lazy val pathToBlpuCSV = config.getString("addressindex.files.csv.blpu")
  private lazy val pathToClassificationCSV = config.getString("addressindex.files.csv.classification")
  private lazy val pathToCrossrefCSV = config.getString("addressindex.files.csv.crossref")
  private lazy val pathToLpiCSV = config.getString("addressindex.files.csv.lpi")
  private lazy val pathToOrganisationCSV = config.getString("addressindex.files.csv.organisation")
  private lazy val pathToStreetCSV = config.getString("addressindex.files.csv.street")
  private lazy val pathToStreetDescriptorCSV = config.getString("addressindex.files.csv.street-descriptor")
  private lazy val pathToSuccessorCSV = config.getString("addressindex.files.csv.successor")

  /**
    * Reads csv into a `DataFrame`
    *
    * @return `DataFrame` containing the delivery point data from CSV
    */
  def readDeliveryPointCSV(): DataFrame = SparkProvider.sqlContext.read
    .format("com.databricks.spark.csv")
    .schema(CSVSchemas.postcodeAddressFileSchema)
    .option("header", "true") // Use first line of all files as header
    .load(resolveAbsolutePath(pathToCsv))

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the blpu data from CSV
    */
  def readBlpuCSV(): DataFrame = SparkProvider.sqlContext.read
    .format("com.databricks.spark.csv")
    .schema(CSVSchemas.blpuFileSchema)
    .option("header", "true")
    .load(resolveAbsolutePath(pathToBlpuCSV))

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the classification data from CSV
    */
  def readClassificationCSV(): DataFrame = SparkProvider.sqlContext.read
    .format("com.databricks.spark.csv")
    .schema(CSVSchemas.classificationFileSchema)
    .option("header", "true")
    .load(resolveAbsolutePath(pathToClassificationCSV))

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the crossref data from CSV
    */
  def readCrossrefCSV(): DataFrame = SparkProvider.sqlContext.read
    .format("com.databricks.spark.csv")
    .schema(CSVSchemas.crossrefFileSchema)
    .option("header", "true")
    .load(resolveAbsolutePath(pathToCrossrefCSV))

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the lpi data from CSV
    */
  def readLpiCSV(): DataFrame = SparkProvider.sqlContext.read
    .format("com.databricks.spark.csv")
    .schema(CSVSchemas.lpiFileSchema)
    .option("header", "true")
    .load(resolveAbsolutePath(pathToLpiCSV))

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the organisation data from CSV
    */
  def readOrganisationCSV(): DataFrame = SparkProvider.sqlContext.read
    .format("com.databricks.spark.csv")
    .schema(CSVSchemas.organisationFileSchema)
    .option("header", "true")
    .load(resolveAbsolutePath(pathToOrganisationCSV))

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the street data from CSV
    */
  def readStreetCSV(): DataFrame = SparkProvider.sqlContext.read
    .format("com.databricks.spark.csv")
    .schema(CSVSchemas.streetFileSchema)
    .option("header", "true")
    .load(resolveAbsolutePath(pathToStreetCSV))

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the street-descriptor data from CSV
    */
  def readStreetDescriptorCSV(): DataFrame = SparkProvider.sqlContext.read
    .format("com.databricks.spark.csv")
    .schema(CSVSchemas.streetDescriptorFileSchema)
    .option("header", "true")
    .load(resolveAbsolutePath(pathToStreetDescriptorCSV))

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the successor data from CSV
    */
  def readSuccessorCSV(): DataFrame = SparkProvider.sqlContext.read
    .format("com.databricks.spark.csv")
    .schema(CSVSchemas.successorFileSchema)
    .option("header", "true")
    .load(resolveAbsolutePath(pathToSuccessorCSV))

  private def resolveAbsolutePath(path: String) =
    if (path.startsWith("hdfs://")) path
    else {
      if (System.getProperty("os.name").toLowerCase.startsWith("windows")) {
        val currentDirectory = new java.io.File(".").getCanonicalPath
        s"$currentDirectory/$path"
      }
      else {
        val currentDirectory = new java.io.File(".").getCanonicalPath
        s"file://$currentDirectory/$path"
      }
    }
}
