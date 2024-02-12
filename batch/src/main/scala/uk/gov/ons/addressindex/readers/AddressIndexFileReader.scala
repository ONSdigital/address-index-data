package uk.gov.ons.addressindex.readers

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import uk.gov.ons.addressindex.models.CSVSchemas
import uk.gov.ons.addressindex.utils.SparkProvider

import scala.util.Try

/**
  * Contains static methods to read different cvs files related to the Address Index
  */
object AddressIndexFileReader {

  lazy val config: Config = ConfigFactory.load()
  lazy val pathToDeliveryPointCsv: String = config.getString("addressindex.files.csv.delivery-point")
  lazy val pathToBlpuCSV: String = config.getString("addressindex.files.csv.blpu")
  lazy val pathToClassificationCSV: String = config.getString("addressindex.files.csv.classification")
  lazy val pathToCrossrefCSV: String = config.getString("addressindex.files.csv.crossref")
  lazy val pathToLpiCSV: String = config.getString("addressindex.files.csv.lpi")
  lazy val pathToOrganisationCSV: String = config.getString("addressindex.files.csv.organisation")
  lazy val pathToStreetCSV: String = config.getString("addressindex.files.csv.street")
  lazy val pathToStreetDescriptorCSV: String = config.getString("addressindex.files.csv.street-descriptor")
  lazy val pathToSuccessorCSV: String = config.getString("addressindex.files.csv.successor")
  lazy val pathToHierarchyCSV: String = config.getString("addressindex.files.csv.hierarchy")
  lazy val pathToDeliveryPointCsv2: String = config.getString("addressindex.files.csv.delivery-point_islands")
  lazy val pathToBlpuCSV2: String = config.getString("addressindex.files.csv.blpu_islands")
  lazy val pathToClassificationCSV2: String = config.getString("addressindex.files.csv.classification_islands")
  lazy val pathToCrossrefCSV2: String = config.getString("addressindex.files.csv.crossref_islands")
  lazy val pathToLpiCSV2: String = config.getString("addressindex.files.csv.lpi_islands")
  lazy val pathToOrganisationCSV2: String = config.getString("addressindex.files.csv.organisation_islands")
  lazy val pathToStreetCSV2: String = config.getString("addressindex.files.csv.street_islands")
  lazy val pathToStreetDescriptorCSV2: String = config.getString("addressindex.files.csv.street-descriptor_islands")
  lazy val pathToSuccessorCSV2: String = config.getString("addressindex.files.csv.successor_islands")
  lazy val pathToHierarchyCSV2: String = config.getString("addressindex.files.csv.hierarchy_islands")
  lazy val pathToRDMFCSV: String = config.getString("addressindex.files.csv.rdmf")

  lazy val isIslands: Boolean = Try(config.getString("addressindex.islands.used").toBoolean).getOrElse(false)

  /**
    * Reads csv into a `DataFrame`
    *
    * @return `DataFrame` containing the RDMF extract data
    */
  def readRDMFCSV(): DataFrame = readCsv(pathToRDMFCSV, CSVSchemas.rdmfFileSchema)

  /**
    * Reads csv into a `DataFrame`
    *
    * @return `DataFrame` containing the delivery point data from CSV
    */
  def readDeliveryPointCSV(): DataFrame = {
    if (isIslands)
      readCsv2(pathToDeliveryPointCsv, pathToDeliveryPointCsv2, CSVSchemas.postcodeAddressFileSchema)
    else
      readCsv(pathToDeliveryPointCsv, CSVSchemas.postcodeAddressFileSchema)
  }

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the blpu data from CSV
    */
  def readBlpuCSV(): DataFrame = {
    if (isIslands)
      readCsv2(pathToBlpuCSV, pathToBlpuCSV2, CSVSchemas.blpuFileSchema)
    else
      readCsv(pathToBlpuCSV, CSVSchemas.blpuFileSchema)
  }

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the classification data from CSV
    */
  def readClassificationCSV(): DataFrame = {
    if (isIslands)
      readCsv2(pathToClassificationCSV, pathToClassificationCSV2, CSVSchemas.classificationFileSchema)
    else
      readCsv(pathToClassificationCSV, CSVSchemas.classificationFileSchema)
  }

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the crossref data from CSV
    */
  def readCrossrefCSV(): DataFrame = {
    if (isIslands)
      readCsv2(pathToCrossrefCSV, pathToCrossrefCSV2, CSVSchemas.crossrefFileSchema)
    else
      readCsv(pathToCrossrefCSV, CSVSchemas.crossrefFileSchema)
  }

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the lpi data from CSV
    */
  def readLpiCSV(): DataFrame = {
    if (isIslands)
      readCsv2(pathToLpiCSV, pathToLpiCSV2, CSVSchemas.lpiFileSchema)
    else
      readCsv(pathToLpiCSV, CSVSchemas.lpiFileSchema)
  }

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the organisation data from CSV
    */
  def readOrganisationCSV(): DataFrame = {
    if (isIslands)
      readCsv2(pathToOrganisationCSV, pathToOrganisationCSV2, CSVSchemas.organisationFileSchema)
    else
      readCsv(pathToOrganisationCSV, CSVSchemas.organisationFileSchema)
  }

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the street data from CSV
    */
  def readStreetCSV(): DataFrame = {
    if (isIslands)
      readCsv2(pathToStreetCSV, pathToStreetCSV2, CSVSchemas.streetFileSchema)
    else
      readCsv(pathToStreetCSV, CSVSchemas.streetFileSchema)
  }

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the street-descriptor data from CSV
    */
  def readStreetDescriptorCSV(): DataFrame = {
    if (isIslands)
      readCsv2(pathToStreetDescriptorCSV, pathToStreetDescriptorCSV2, CSVSchemas.streetDescriptorFileSchema)
    else
      readCsv(pathToStreetDescriptorCSV, CSVSchemas.streetDescriptorFileSchema)
  }

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the successor data from CSV
    */
  def readSuccessorCSV(): DataFrame = {
    if (isIslands)
      readCsv2(pathToSuccessorCSV, pathToSuccessorCSV2, CSVSchemas.successorFileSchema)
    else
      readCsv(pathToSuccessorCSV, CSVSchemas.successorFileSchema)
  }

  /**
    * Reads csv into a 'DataFrame'
    *
    * @return 'DataFrame' containing the hierarchy data from CSV
    */
  def readHierarchyCSV(): DataFrame = {
    if (isIslands)
      readCsv2(pathToHierarchyCSV, pathToHierarchyCSV2, CSVSchemas.hierarchyFileSchema)
    else
      readCsv(pathToHierarchyCSV, CSVSchemas.hierarchyFileSchema)
  }

  private def readCsv(path1: String, schema: StructType): DataFrame =
    SparkProvider.sparkContext.read
      .format("com.databricks.spark.csv")
      .schema(schema)
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .load(resolveAbsolutePath(path1))

  private def readCsv2(path1: String, path2: String, schema: StructType): DataFrame = {
     SparkProvider.sparkContext.read
      .format("com.databricks.spark.csv")
      .schema(schema)
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .load(resolveAbsolutePath(path1), resolveAbsolutePath(path2))
  }

    private def readTxt(path: String, schema: StructType): DataFrame =
    SparkProvider.sparkContext.read
      .format("com.databricks.spark.csv")
      .schema(schema)
      .option("header", "true")
      .option("delimiter", "|")
      .option("mode", "PERMISSIVE")
      .load(resolveAbsolutePath(path))

  private def resolveAbsolutePath(path: String) = {
    val currentDirectory = new java.io.File(".").getCanonicalPath

    if (path.startsWith("hdfs://") || path.startsWith("gs://") ) path
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
  val nameRegex1 = s"ABP_E$epoch.+_v$date\\.csv$$".r
  val nameRegex2 = s"ABP_E$epoch.+_v$date\\.csv\\.gz$$".r

    if (nameRegex1.findFirstIn(filePath).isDefined || nameRegex2.findFirstIn(filePath).isDefined) true
    else
      true
      // throw new IllegalArgumentException(s"file $filePath does not contain epoch $epoch and date $date")
    }

  // this will remain hard coded unless the epoch number appears in the filename instead of "current"
  // alernatively could use the epoch number from the rmdf lookup file
  def extractEpoch(filePath: String): Int = {
//    val epochRegex = s"ABP_E(\\d+).+$$".r
//    val epoch = epochRegex.findFirstMatchIn(filePath).getOrElse(throw new IllegalArgumentException(s"file $filePath does not contain epoch number"))
//    epoch.group(1).toInt
    106
  }

  def extractDate(filePath: String): String = {
    val dateRegex1 = s"ai_aims.+(\\d{8})\\.csv$$".r
    val date = dateRegex1.findFirstMatchIn(filePath).getOrElse(throw new IllegalArgumentException(s"file $filePath does not contain valid date"))
        date.group(1)
  }

  def generateIndexNameFromFileName(historical : Boolean = true, skinny : Boolean = false): String = {
    val epoch = extractEpoch(pathToDeliveryPointCsv)
    val date = extractDate(pathToDeliveryPointCsv)

    val baseIndexName =
      if (historical) {
        config.getString("addressindex.elasticsearch.indices.hybrid")
      } else {
        config.getString("addressindex.elasticsearch.indices.hybridHistorical")
      }

    val subIndex =
      if (skinny) config.getString("addressindex.elasticsearch.indices.skinny") else ""

    s"$baseIndexName${subIndex}_${epoch}_${date}_${System.currentTimeMillis()}"
  }
}