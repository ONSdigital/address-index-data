package uk.gov.ons.addressindex

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.rogach.scallop.ScallopConf
import scalaj.http.{Http, HttpResponse}
import uk.gov.ons.addressindex.readers.AddressIndexFileReader
import uk.gov.ons.addressindex.utils.{Mappings, SqlHelper}
import uk.gov.ons.addressindex.writers.ElasticSearchWriter

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

  val nodes = config.getString("addressindex.elasticsearch.nodes")
  val port = config.getString("addressindex.elasticsearch.port")

  // each run of this application has a unique index name
  val indexName =
    if (opts.hybridNoHist()) {
      generateIndexName(false)
    } else {
      generateIndexName()
    }

  val url = s"http://$nodes:$port/$indexName"

  if (!opts.help()) {
    AddressIndexFileReader.validateFileNames()

    if (opts.mapping()) postMapping(indexName)
    if (opts.hybrid()) {
      preLoad(indexName)
      saveHybridAddresses()
      postLoad(indexName)
    }
    if (opts.hybridNoHist()) {
      preLoad(indexName)
      saveHybridAddresses(false)
      postLoad(indexName)
    }

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

  private def saveHybridAddresses(historical : Boolean = true) = {

    val nag = generateNagAddresses(historical)
    val paf = AddressIndexFileReader.readDeliveryPointCSV()
    val hybrid = SqlHelper.aggregateHybridIndex(paf, nag, historical)

    ElasticSearchWriter.saveHybridAddresses(s"$indexName/address", hybrid)
  }

  private def postMapping(indexName: String) = {
    val response: HttpResponse[String] = Http(url)
      .put(Mappings.hybrid)
      .header("Content-type", "application/json")
      .asString
    if (response.code != 200) throw new Exception(s"Could not create mapping using PUT: code ${response.code} body ${response.body}")
  }

  private def preLoad(indexName: String) = {
    val refreshResponse: HttpResponse[String] = Http(url + "/_settings")
      .put("""{"index":{"refresh_interval":"-1"}}""")
      .asString
    if (refreshResponse.code != 200) throw new Exception(s"Could not set refresh interval using PUT: code ${refreshResponse.code} body ${refreshResponse.body}")
  }

  private def postLoad(indexName: String) = {
    val replicaResponse: HttpResponse[String] = Http(url + "/_settings")
      .put("""{"index":{"number_of_replicas":1}}""")
      .asString
    if (replicaResponse.code != 200) throw new Exception(s"Could not set number of replicas using PUT: code ${replicaResponse.code} body ${replicaResponse.body}")
    val refreshResponse: HttpResponse[String] = Http(url + "/_settings")
      .put("""{"index":{"refresh_interval":"1s"}}""")
      .asString
    if (refreshResponse.code != 200) throw new Exception(s"Could not set refresh interval using PUT: code ${refreshResponse.code} body ${refreshResponse.body}")
  }
}