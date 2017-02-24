package uk.gov.ons.addressindex

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.rogach.scallop.ScallopConf
import uk.gov.ons.addressindex.readers.AddressIndexFileReader
import uk.gov.ons.addressindex.utils.SqlHelper
import uk.gov.ons.addressindex.writers.ElasticSearchWriter
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.{DefaultHttpClient, HttpClientBuilder}
import org.apache.http.entity.StringEntity

import scala.io.Source

/**
 * Main executed file
 */
object Main extends App {
  val opts = new ScallopConf(args) {
    banner(
      """
PAF and NAG indexer. All options are mutually exclusive.

Example: java -jar ons-ai-batch.jar --paf

For usage see below:
      """)

    val paf = opt[Boolean]("paf", noshort = true, descr = "Index PAF")
    val nag = opt[Boolean]("nag", noshort = true, descr = "Index NAG")
    val hybrid = opt[Boolean]("hybrid", noshort = true, descr = "Index hybrid PAF & NAG")
    val help = opt[Boolean]("help", noshort = true, descr = "Show this message")
    mutuallyExclusive(paf, nag, hybrid, help)
    verify()
  }

  if (!opts.help()) {
    if (opts.paf()) {
      savePafAddresses()
    } else if (opts.nag()) {
      saveNagAddresses()
    } else if (opts.hybrid()) {
      saveHybridAddresses()
    } else {
      opts.printHelp()
    }
  }

  private def saveNagAddresses() = {
    val resultDF = generateNagAddresses()
    ElasticSearchWriter.saveNAGAddresses(resultDF)
  }

  private def generateNagAddresses(): DataFrame = {
    val blpu = AddressIndexFileReader.readBlpuCSV()
    val lpi = AddressIndexFileReader.readLpiCSV()
    val organisation = AddressIndexFileReader.readOrganisationCSV()
    val classification = AddressIndexFileReader.readClassificationCSV()
    val street = AddressIndexFileReader.readStreetCSV()
    val streetDescriptor = AddressIndexFileReader.readStreetDescriptorCSV()
    val crossRef = AddressIndexFileReader.readCrossrefCSV()
    SqlHelper.joinCsvs(blpu, lpi, organisation, classification, street, streetDescriptor, crossRef)
  }

  private def savePafAddresses() = {
    val csv = AddressIndexFileReader.readDeliveryPointCSV()
    ElasticSearchWriter.savePAFAddresses(csv)
  }

  private def saveHybridAddresses() = {
    val nag = generateNagAddresses()
    val paf = AddressIndexFileReader.readDeliveryPointCSV()
    val hybrid = SqlHelper.aggregateHybridIndex(paf, nag)

    postMapping()
    ElasticSearchWriter.saveHybridAddresses(hybrid)
  }

  private def postMapping() = {
    val config = ConfigFactory.load()
    val url = s"http://${config.getString("addressindex.elasticsearch.nodes")}:" +
      s"${config.getString("addressindex.elasticsearch.port")}/" +
      s"${config.getString("addressindex.elasticsearch.indices.hybrid")}${System.currentTimeMillis()}/address"
    val fileContents = Source.fromFile(config.getString("addressindex.files.es.json")).getLines.mkString

    val post = new HttpPost(url)
    post.setHeader("Content-type", "application/json")
    post.setEntity(new StringEntity(fileContents))

    HttpClientBuilder.create.build.execute(post)
  }
}