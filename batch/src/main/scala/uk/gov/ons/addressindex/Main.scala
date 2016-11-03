package uk.gov.ons.addressindex

import uk.gov.ons.addressindex.readers.AddressIndexFileReader
import uk.gov.ons.addressindex.writers.ElasticSearchWriter

/**
  * Main executed file
  */
object Main extends App{
  val csv = AddressIndexFileReader.readDeliveryPointCSV("delivery_point/read_test.csv")
  ElasticSearchWriter.saveAddresses(csv)
}
