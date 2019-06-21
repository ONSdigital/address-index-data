package uk.gov.ons.addressindex

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.rogach.scallop.{ScallopConf, ScallopOption}
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

    val hybrid: ScallopOption[Boolean] = opt("hybrid", noshort = true, descr = "Index hybrid PAF & NAG including historical data")
    val hybridNoHist: ScallopOption[Boolean] = opt("hybridNoHist", noshort = true, descr = "Index hybrid PAF & NAG no historical data")
    val mapping: ScallopOption[Boolean] = opt("mapping", noshort = true, descr = "Creates mapping for the index")
    val help: ScallopOption[Boolean] = opt("help", noshort = true, descr = "Show this message")
    val skinny: ScallopOption[Boolean] = opt("skinny", noshort = true, descr = "Create a skinny index")
    val nisra: ScallopOption[Boolean] = opt("nisra", noshort = true, descr = "Include NISRA data")
    verify()
  }

  val nodes = config.getString("addressindex.elasticsearch.nodes")
  val port = config.getString("addressindex.elasticsearch.port")

  // each run of this application has a unique index name
  val indexName = generateIndexName(historical = !opts.hybridNoHist(), skinny = opts.skinny(), nisra = opts.nisra())

  val url = s"http://$nodes:$port/$indexName"

  if (!opts.help()) {
    AddressIndexFileReader.validateFileNames()

    postMapping(indexName, skinny = opts.skinny())
    preLoad(indexName)
    saveHybridAddresses(historical = !opts.hybridNoHist(), skinny = opts.skinny(), nisra = opts.nisra())
    postLoad(indexName)
  } else opts.printHelp()

  // val indexName = generateIndexName(historical = true, skinny = true, nisra = true)
  // val url = s"http://$nodes:$port/$indexName"
  // postMapping(indexName, skinny = true)
  // saveHybridAddresses(historical = true, skinny = true, nisra = true)

  private def generateIndexName(historical: Boolean = true, skinny: Boolean = false, nisra: Boolean = false): String =
    AddressIndexFileReader.generateIndexNameFromFileName(historical, skinny, nisra)

  private def generateNagAddresses(historical: Boolean = true, skinny: Boolean = false): DataFrame = {
    val blpu = AddressIndexFileReader.readBlpuCSV()
    val classification = AddressIndexFileReader.readClassificationCSV()
    val lpi = AddressIndexFileReader.readLpiCSV()
    val organisation = AddressIndexFileReader.readOrganisationCSV()
    val street = AddressIndexFileReader.readStreetCSV()
    val streetDescriptor = AddressIndexFileReader.readStreetDescriptorCSV()
    SqlHelper.joinCsvs(blpu, classification, lpi, organisation, street, streetDescriptor, historical, skinny)
  }

  private def saveHybridAddresses(historical: Boolean = true, skinny: Boolean = false, nisra: Boolean = false): Unit = {
    val nag = generateNagAddresses(historical, skinny)
    val paf = AddressIndexFileReader.readDeliveryPointCSV()
    val nisratxt = AddressIndexFileReader.readNisraTXT()

    if (nisra) {
      if (skinny) {
        ElasticSearchWriter.saveSkinnyHybridNisraAddresses(s"$indexName/address", SqlHelper.aggregateHybridSkinnyNisraIndex(paf, nag, nisratxt, historical))
      } else {
        ElasticSearchWriter.saveHybridNisraAddresses(s"$indexName/address", SqlHelper.aggregateHybridNisraIndex(paf, nag, nisratxt, historical))
      }
    } else {
      if (skinny) {
        ElasticSearchWriter.saveSkinnyHybridAddresses(s"$indexName/address", SqlHelper.aggregateHybridSkinnyIndex(paf, nag, historical))
      } else {
        ElasticSearchWriter.saveHybridAddresses(s"$indexName/address", SqlHelper.aggregateHybridIndex(paf, nag, historical))
      }
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
      .asString
    if (response.code != 200) throw new Exception(s"Could not create mapping using PUT: code ${response.code} body ${response.body}")
  }

  private def preLoad(indexName: String): Unit = {
    val refreshResponse: HttpResponse[String] = Http(url + "/_settings")
      .put("""{"index":{"refresh_interval":"-1"}}""")
      .asString
    if (refreshResponse.code != 200) throw new Exception(s"Could not set refresh interval using PUT: code ${refreshResponse.code} body ${refreshResponse.body}")
  }

  private def postLoad(indexName: String): Unit = {
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