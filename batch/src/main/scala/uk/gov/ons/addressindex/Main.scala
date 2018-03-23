package uk.gov.ons.addressindex

import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.rogach.scallop.ScallopConf
import uk.gov.ons.addressindex.models.{CrossRefDocument, HierarchyDocument}
import uk.gov.ons.addressindex.readers.AddressIndexFileReader
import uk.gov.ons.addressindex.utils.{Mappings, SqlHelper}
import uk.gov.ons.addressindex.writers.ElasticSearchWriter

import scalaj.http.{Http, HttpResponse}

/**
 * Main executed file
 */
object Main extends App {

  val config = ConfigFactory.load()

  val opts = new ScallopConf(args) {
    banner(
      """
Hybrid indexer. All options are mutually exclusive.

Example: java -jar ons-ai-batch.jar --mapping --hybrid

For usage see below:
      """)

    val hybrid = opt[Boolean]("hybrid", noshort = true, descr = "Index hybrid PAF & NAG including historical data")
    val hybridNoHist = opt[Boolean]("hybridNoHist", noshort = true, descr = "Index hybrid PAF & NAG no historical data")
    val mapping = opt[Boolean]("mapping", noshort = true, descr = "Creates mapping for the index")
    val help = opt[Boolean]("help", noshort = true, descr = "Show this message")
    verify()
  }

  // each run of this application has a unique index name
  val indexName =
    if (opts.hybridNoHist()) {
      generateIndexName(false)
    } else {
      generateIndexName()
    }

  if (!opts.help()) {
    AddressIndexFileReader.validateFileNames()

    if (opts.mapping()) postMapping(indexName)
    if (opts.hybrid()) saveHybridAddresses()
    if (opts.hybridNoHist()) saveHybridAddresses(false)

  } else opts.printHelp()

  private def generateIndexName(historical : Boolean = true): String = AddressIndexFileReader.generateIndexNameFromFileName(historical)

  private def generateNagAddresses(historical : Boolean = true): DataFrame = {
    val blpu = AddressIndexFileReader.readBlpuCSV()
    val lpi = AddressIndexFileReader.readLpiCSV()
    val organisation = AddressIndexFileReader.readOrganisationCSV()
    val classification = AddressIndexFileReader.readClassificationCSV()
    val street = AddressIndexFileReader.readStreetCSV()
    val streetDescriptor = AddressIndexFileReader.readStreetDescriptorCSV()
    SqlHelper.joinCsvs(blpu, lpi, organisation, classification, street, streetDescriptor, historical)
  }

  private def generateHierarchyData(): RDD[HierarchyDocument] = {
    val hierarchyData = AddressIndexFileReader.readHierarchyCSV()
    val hierarchyGrouped = SqlHelper.aggregateHierarchyInformation(hierarchyData)
    SqlHelper.constructHierarchyRdd(hierarchyData, hierarchyGrouped)
  }

  private def generateCrossRefData(): RDD[CrossRefDocument] = {
    val crossRefData = AddressIndexFileReader.readCrossrefCSV()
    val crossRefGrouped = SqlHelper.aggregateCrossRefInformation(crossRefData)
    SqlHelper.constructCrossRefRdd(crossRefData, crossRefGrouped)
  }

  private def saveHybridAddresses(historical : Boolean = true) = {

    val nag = generateNagAddresses(historical)
    val paf = AddressIndexFileReader.readDeliveryPointCSV()
    val hierarchy = generateHierarchyData()
    val crossRef = generateCrossRefData()
    val hybrid = SqlHelper.aggregateHybridIndex(paf, nag, hierarchy, crossRef, historical)

    ElasticSearchWriter.saveHybridAddresses(s"$indexName/address", hybrid)
  }

  private def postMapping(indexName: String) = {
    val nodes = config.getString("addressindex.elasticsearch.nodes")
    val port = config.getString("addressindex.elasticsearch.port")
    val url = s"http://$nodes:$port/$indexName"

    val response: HttpResponse[String] = Http(url).put(Mappings.hybrid).header("Content-type", "application/json").asString
    if (response.code != 200) throw new Exception(s"Could not create mapping using PUT: code ${response.code} body ${response.body}")
  }
}