package uk.gov.ons.addressindex.readers

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
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
  def readDeliveryPointCSV(): DataFrame = getDataFrame(pathToCsv, CSVSchemas.postcodeAddressFileSchema)

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the blpu data from CSV
    */
  def readBlpuCSV(): DataFrame = getDataFrame(pathToBlpuCSV, CSVSchemas.blpuFileSchema)

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the classification data from CSV
    */
  def readClassificationCSV(): DataFrame = getDataFrame(pathToClassificationCSV, CSVSchemas.classificationFileSchema)

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the crossref data from CSV
    */
  def readCrossrefCSV(): DataFrame = getDataFrame(pathToCrossrefCSV, CSVSchemas.crossrefFileSchema)

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the lpi data from CSV
    */
  def readLpiCSV(): DataFrame = getDataFrame(pathToLpiCSV, CSVSchemas.lpiFileSchema)

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the organisation data from CSV
    */
  def readOrganisationCSV(): DataFrame = getDataFrame(pathToOrganisationCSV, CSVSchemas.organisationFileSchema)

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the street data from CSV
    */
  def readStreetCSV(): DataFrame = getDataFrame(pathToStreetCSV, CSVSchemas.streetFileSchema)

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the street-descriptor data from CSV
    */
  def readStreetDescriptorCSV(): DataFrame = getDataFrame(pathToStreetDescriptorCSV, CSVSchemas.streetDescriptorFileSchema)

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the successor data from CSV
    */
  def readSuccessorCSV(): DataFrame = getDataFrame(pathToSuccessorCSV, CSVSchemas.successorFileSchema)

  private def getDataFrame(path: String, schema: StructType) =
    SparkProvider.sqlContext.read
      .format("com.databricks.spark.csv")
      .schema(schema)
      .option("header", "true")
      .load(resolveAbsolutePath(path))

  private def resolveAbsolutePath(path: String) = {

    val currentDirectory = new java.io.File(".").getCanonicalPath

    if (path.startsWith("hdfs://")) path
    else {
      if (System.getProperty("os.name").toLowerCase.startsWith("windows")) {
        currentDirectory.concat(s"/$path")
      }
      else {
        currentDirectory.concat(s"file://$path")
      }
    }
  }
}
