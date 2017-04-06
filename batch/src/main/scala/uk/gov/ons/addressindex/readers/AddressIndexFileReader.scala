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

  lazy val config = ConfigFactory.load()
  lazy val pathToDeliveryPointCsv = config.getString("addressindex.files.csv.delivery-point")
  lazy val pathToBlpuCSV = config.getString("addressindex.files.csv.blpu")
  lazy val pathToClassificationCSV = config.getString("addressindex.files.csv.classification")
  lazy val pathToCrossrefCSV = config.getString("addressindex.files.csv.crossref")
  lazy val pathToLpiCSV = config.getString("addressindex.files.csv.lpi")
  lazy val pathToOrganisationCSV = config.getString("addressindex.files.csv.organisation")
  lazy val pathToStreetCSV = config.getString("addressindex.files.csv.street")
  lazy val pathToStreetDescriptorCSV = config.getString("addressindex.files.csv.street-descriptor")
  lazy val pathToSuccessorCSV = config.getString("addressindex.files.csv.successor")
  lazy val pathToHierarchyCSV = config.getString("addressindex.files.csv.hierarchy")

  /**
    * Reads csv into a `DataFrame`
    *
    * @return `DataFrame` containing the delivery point data from CSV
    */
  def readDeliveryPointCSV(): DataFrame = readCsv(pathToDeliveryPointCsv, CSVSchemas.postcodeAddressFileSchema)

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the blpu data from CSV
    */
  def readBlpuCSV(): DataFrame = readCsv(pathToBlpuCSV, CSVSchemas.blpuFileSchema)

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the classification data from CSV
    */
  def readClassificationCSV(): DataFrame = readCsv(pathToClassificationCSV, CSVSchemas.classificationFileSchema)

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the crossref data from CSV
    */
  def readCrossrefCSV(): DataFrame = readCsv(pathToCrossrefCSV, CSVSchemas.crossrefFileSchema)

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the lpi data from CSV
    */
  def readLpiCSV(): DataFrame = readCsv(pathToLpiCSV, CSVSchemas.lpiFileSchema)

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the organisation data from CSV
    */
  def readOrganisationCSV(): DataFrame = readCsv(pathToOrganisationCSV, CSVSchemas.organisationFileSchema)

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the street data from CSV
    */
  def readStreetCSV(): DataFrame = readCsv(pathToStreetCSV, CSVSchemas.streetFileSchema)

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the street-descriptor data from CSV
    */
  def readStreetDescriptorCSV(): DataFrame = readCsv(pathToStreetDescriptorCSV, CSVSchemas.streetDescriptorFileSchema)

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the successor data from CSV
    */
  def readSuccessorCSV(): DataFrame = readCsv(pathToSuccessorCSV, CSVSchemas.successorFileSchema)

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the hierarchy data from CSV
    */
  def readHierarchyCSV(): DataFrame = readCsv(pathToHierarchyCSV, CSVSchemas.hierarchyFileSchema)

  private def readCsv(path: String, schema: StructType): DataFrame =
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
        s"$currentDirectory/$path"
      }
      else {
        s"file://$currentDirectory/$path"
      }
    }
  }

  def validateFileNames(): Boolean = {

    val epoch = extractEpoch(pathToDeliveryPointCsv)
    val date = extractDate(pathToDeliveryPointCsv)

    Seq(
      pathToDeliveryPointCsv,
      pathToBlpuCSV,
      pathToClassificationCSV,
      pathToCrossrefCSV,
      pathToLpiCSV,
      pathToOrganisationCSV,
      pathToStreetCSV,
      pathToStreetDescriptorCSV,
      pathToSuccessorCSV,
      pathToHierarchyCSV
    ).forall(fileName => validateFileName(fileName, epoch, date))

  }

  def validateFileName(filePath: String, epoch: Int, date: String): Boolean = {
    val nameRegex = s"ABP_E$epoch.+_v$date$$".r

    if (nameRegex.findFirstIn(filePath).isDefined) true
    else throw new IllegalArgumentException(s"file $filePath does not contain epoch $epoch and date $date")
  }

  def extractEpoch(filePath: String): Int = {
    val epochRegex = s"ABP_E(\\d+).+$$".r
    val epoch = epochRegex.findFirstMatchIn(filePath).getOrElse(throw new IllegalArgumentException(s"file $filePath does not contain epoch number"))
    epoch.group(1).toInt
  }

  def extractDate(filePath: String): String ={
    val dateRegex = s"ABP_E.+(\\d{6})\\.csv$$".r
    val date = dateRegex.findFirstMatchIn(filePath).getOrElse(throw new IllegalArgumentException(s"file $filePath does not contain valid date"))
    date.group(1)
  }

  def generateIndexNameFromFileName(): String = {
    val epoch = extractEpoch(pathToDeliveryPointCsv)
    val date = extractDate(pathToDeliveryPointCsv)

    val baseIndexName = config.getString("addressindex.elasticsearch.indices.hybrid")
    s"${baseIndexName}_${epoch}_${date}_${System.currentTimeMillis()}"
  }
}