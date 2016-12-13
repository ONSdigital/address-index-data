package uk.gov.ons.addressindex

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
PAF and NAG indexer

Example: java -jar ons-ai-batch.jar --paf

For usage see below:
      """)

    val paf = opt[Boolean]("paf", descr = "Index PAF")
    val nag = opt[Boolean]("nag", descr = "Index NAG")
    val both = opt[Boolean]("both", descr = "Index both PAF and NAG")
    val help = opt[Boolean]("help", noshort = true, descr = "Show this message")
    mutuallyExclusive(paf, nag, both, help)
    verify()
  }

  if (!opts.help()) {
    if (opts.paf()) {
      savePAFAddresses()
    } else if (opts.nag()) {
      saveNAGAddresses()
    } else {
      savePAFAddresses()
      saveNAGAddresses()
    }
  }

  private def saveNAGAddresses() = {
    val blpu = AddressIndexFileReader.readBlpuCSV()
    val lpi = AddressIndexFileReader.readLpiCSV()
    val organisation = AddressIndexFileReader.readOrganisationCSV()
    val classification = AddressIndexFileReader.readClassificationCSV()
    val street = AddressIndexFileReader.readStreetCSV()
    val streetDescriptor = AddressIndexFileReader.readStreetDescriptorCSV()
    val resultDF = SqlHelper.joinCsvs(blpu, lpi, organisation, classification, street, streetDescriptor)
    ElasticSearchWriter.saveNAGAddresses(resultDF)
  }

  private def savePAFAddresses() = {
    val csv = AddressIndexFileReader.readDeliveryPointCSV()
    ElasticSearchWriter.savePAFAddresses(csv)
  }
}
