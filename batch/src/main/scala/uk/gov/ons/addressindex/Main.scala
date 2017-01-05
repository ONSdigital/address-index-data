package uk.gov.ons.addressindex

import org.apache.spark.sql.DataFrame
import org.rogach.scallop.ScallopConf
import uk.gov.ons.addressindex.readers.AddressIndexFileReader
import uk.gov.ons.addressindex.utils.SqlHelper
import uk.gov.ons.addressindex.writers.ElasticSearchWriter

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
    } else if (opts.hybrid()){
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
    SqlHelper.joinCsvs(blpu, lpi, organisation, classification, street, streetDescriptor)
  }

  private def savePafAddresses() = {
    val csv = AddressIndexFileReader.readDeliveryPointCSV()
    ElasticSearchWriter.savePAFAddresses(csv)
  }

  private def saveHybridAddresses() = {
    val nag = generateNagAddresses()
    val paf = AddressIndexFileReader.readDeliveryPointCSV()

    val hybrid = SqlHelper.aggregateHybridIndex(paf, nag)
    ElasticSearchWriter.saveHybridAddresses(hybrid)
  }
}
