package uk.gov.ons.addressindex

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.rogach.scallop.ScallopConf
import uk.gov.ons.addressindex.readers.AddressIndexFileReader
import uk.gov.ons.addressindex.utils.{Mappings, SqlHelper}
import uk.gov.ons.addressindex.writers.ElasticSearchWriter
import org.apache.http.client.methods.HttpPut
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.spark.rdd.RDD
import uk.gov.ons.addressindex.models.HierarchyDocument

/**
 * Main executed file
 */
object Main extends App {

  val config = ConfigFactory.load()

  val opts = new ScallopConf(args) {
    banner(
      """
Hybrid indexer. All options are mutually exclusive.

Example: java -jar ons-ai-batch.jar --hybrid

For usage see below:
      """)

    val hybrid = opt[Boolean]("hybrid", noshort = true, descr = "Index hybrid PAF & NAG")
    val help = opt[Boolean]("help", noshort = true, descr = "Show this message")
    mutuallyExclusive(hybrid, help)
    verify()
  }

//  if (!opts.help()) {
//    if (opts.hybrid()) {
      saveHybridAddresses()
//    } else {
//      opts.printHelp()
//    }
//  }

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

  private def generateHierarchyData(): RDD[HierarchyDocument] = {
    val hierarchyData = AddressIndexFileReader.readHierarchyCSV()
    val hierarchyGrouped = SqlHelper.aggregateHierarchyInformation(hierarchyData)
    SqlHelper.constructHierarchyRdd(hierarchyData, hierarchyGrouped)
  }

  private def saveHybridAddresses() = {
    val baseIndexName = config.getString("addressindex.elasticsearch.indices.hybrid")
    val indexName = s"${baseIndexName}"//_${System.currentTimeMillis()}"
//    postMapping(indexName)

    val nag = generateNagAddresses()
    val paf = AddressIndexFileReader.readDeliveryPointCSV()
    val hierarchy = generateHierarchyData()

    val hybrid = SqlHelper.aggregateHybridIndex(paf, nag, hierarchy)

    ElasticSearchWriter.saveHybridAddresses(s"$indexName/address", hybrid)
  }

  private def postMapping(indexName: String) = {
    val url = s"http://${config.getString("addressindex.elasticsearch.nodes")}:" +
      s"${config.getString("addressindex.elasticsearch.port")}/$indexName"
    val put = new HttpPut(url)
    put.setHeader("Content-type", "application/json")
    put.setEntity(new StringEntity(Mappings.hybrid))
    (new DefaultHttpClient).execute(put)
  }
}