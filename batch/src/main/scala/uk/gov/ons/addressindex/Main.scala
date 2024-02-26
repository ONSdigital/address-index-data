package uk.gov.ons.addressindex

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.rogach.scallop.{ScallopConf, ScallopOption}
import scalaj.http.{Http, HttpResponse}
import uk.gov.ons.addressindex.readers.AddressIndexFileReader
import uk.gov.ons.addressindex.utils.{Mappings, SqlHelper, AuthUtil}
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

    val hybrid: ScallopOption[Boolean] = opt("hybrid", noshort = true, descr = "Index hybrid PAF & NAG including historical data")
    val hybridNoHist: ScallopOption[Boolean] = opt("hybridNoHist", noshort = true, descr = "Index hybrid PAF & NAG no historical data")
    val mapping: ScallopOption[Boolean] = opt("mapping", noshort = true, descr = "Creates mapping for the index")
    val help: ScallopOption[Boolean] = opt("help", noshort = true, descr = "Show this message")
    val skinny: ScallopOption[Boolean] = opt("skinny", noshort = true, descr = "Create a skinny index")
   verify()
  }

  val nodes = config.getString("addressindex.elasticsearch.nodes")
  val port = config.getString("addressindex.elasticsearch.port")

  // username and password should be set in the local application.conf
  // this file is not checked into Git (application_full.conf on Spark server)
  val username = config.getString("addressindex.elasticsearch.user")
  val password = config.getString("addressindex.elasticsearch.pass")
  val authHeader = s"Basic ${AuthUtil.encodeCredentials(username, password)}"

  //  each run of this application has a unique index name
  // comment out for local test - start
  val indexName = generateIndexName(historical = !opts.hybridNoHist(), skinny = opts.skinny())
  val url = s"http://$nodes:$port/$indexName"

  if (!opts.help()) {
    AddressIndexFileReader.validateFileNames()
    postMapping(indexName, skinny = opts.skinny())
    preLoad(indexName)
    saveHybridAddresses(historical = !opts.hybridNoHist(), skinny = opts.skinny())
    postLoad(indexName)
  } else opts.printHelp()
  // comment out for local test - end

  // uncomment for local test - start
  // val indexName = generateIndexName(historical = false, skinny = false)
  // val url = s"http://$nodes:$port/$indexName"
  // postMapping(indexName, skinny = true)
  // saveHybridAddresses(historical = true, skinny = true)
  // uncomment for local test - end

  private def generateIndexName(historical: Boolean = true, skinny: Boolean = false ): String =
    AddressIndexFileReader.generateIndexNameFromFileName(historical, skinny)

  private def generateNagAddresses(historical: Boolean = true, skinny: Boolean = false): DataFrame = {
    val blpu = AddressIndexFileReader.readBlpuCSV()
    val classification = AddressIndexFileReader.readClassificationCSV()
    val lpi = AddressIndexFileReader.readLpiCSV()
    val organisation = AddressIndexFileReader.readOrganisationCSV()
    val street = AddressIndexFileReader.readStreetCSV()
    val streetDescriptor = AddressIndexFileReader.readStreetDescriptorCSV()
    SqlHelper.joinCsvs(blpu, classification, lpi, organisation, street, streetDescriptor, historical, skinny)
  }

  private def saveHybridAddresses(historical: Boolean = true, skinny: Boolean = false): Unit = {
    val nag = generateNagAddresses(historical, skinny)
    val paf = AddressIndexFileReader.readDeliveryPointCSV()

    if (skinny) {
        ElasticSearchWriter.saveSkinnyHybridAddresses(s"$indexName", SqlHelper.aggregateHybridSkinnyIndex(paf, nag,historical))
    } else {
        ElasticSearchWriter.saveHybridAddresses(s"$indexName", SqlHelper.aggregateHybridIndex(paf, nag, historical))
    }

  }

  private def postMapping(indexName: String, skinny: Boolean = false): Unit = {
    val response: HttpResponse[String] = Http(url)
      .put(
        if (skinny) {
          Mappings.hybridSkinny
        } else {
          Mappings.hybrid
        })
      .header("Content-type", "application/json")
      .header("Authorization", authHeader)
      .asString
    if (response.code != 200) throw new Exception(s"Could not create mapping using PUT: code ${response.code} body ${response.body}")
  }

  private def preLoad(indexName: String): Unit = {
    val refreshResponse: HttpResponse[String] = Http(url + "/_settings")
      .put("""{"index":{"refresh_interval":"-1"}}""")
      .header("Content-type", "application/json")
      // .header("WWW-Authenticate","Basic xxcxc")
      .header("Authorization", authHeader)
      .asString
    if (refreshResponse.code != 200) throw new Exception(s"Could not set refresh interval using PUT: code ${refreshResponse.code} body ${refreshResponse.body}")
  }

  private def postLoad(indexName: String): Unit = {
    val replicaResponse: HttpResponse[String] = Http(url + "/_settings")
      .put("""{"index":{"number_of_replicas":1}}""")
      .header("Content-type", "application/json")
      .header("Authorization", authHeader)
      .asString
    if (replicaResponse.code != 200) throw new Exception(s"Could not set number of replicas using PUT: code ${replicaResponse.code} body ${replicaResponse.body}")
    val refreshResponse: HttpResponse[String] = Http(url + "/_settings")
      .put("""{"index":{"refresh_interval":"1s"}}""")
      .header("Content-type", "application/json")
      .header("Authorization", authHeader)
      .asString
    if (refreshResponse.code != 200) throw new Exception(s"Could not set refresh interval using PUT: code ${refreshResponse.code} body ${refreshResponse.body}")
  }
}