package uk.gov.ons.addressindex.utils

import org.apache.spark.sql.DataFrame

/**
  * Join the Csv files into single DataFrame
  */
object SqlHelper {

  def joinCsvs(blpu: DataFrame, lpi: DataFrame, organisation: DataFrame, street: DataFrame,
               streetDescriptor: DataFrame): DataFrame = {

    val blpuTable = SparkProvider.registerTempTable(blpu, "blpu")
    val organisationTable = SparkProvider.registerTempTable(organisation, "organisation")
    val lpiTable = SparkProvider.registerTempTable(lpi, "lpi")
    val streetTable = SparkProvider.registerTempTable(street, "street")
    val streetDescriptorTable = SparkProvider.registerTempTable(streetDescriptor, "street_descriptor")

    SparkProvider.sqlContext.sql(
      s"""SELECT $blpuTable.uprn, $organisationTable.organisation, $lpiTable .paoText,
        |$lpiTable.paoStartNumber, $lpiTable.saoText, $streetDescriptorTable.streetDescriptor,
        |$streetDescriptorTable.townName, $streetDescriptorTable.locality
        |FROM $blpuTable
        |JOIN $organisationTable ON $blpuTable.uprn = $organisationTable.uprn
        |JOIN $lpiTable ON $blpuTable.uprn = $lpiTable.uprn
        |JOIN $streetTable on $lpiTable.usrn = $streetTable.usrn
        |JOIN $streetDescriptorTable on $streetTable.usrn = $streetDescriptorTable.usrn""".stripMargin)
  }
}
