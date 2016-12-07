package uk.gov.ons.addressindex.utils

import org.apache.spark.sql.DataFrame

/**
  * Join the Csv files into single DataFrame
  */
object SqlHelper {

  def joinCsvs(blpu: DataFrame, lpi: DataFrame, organisation: DataFrame, street: DataFrame,
               streetDescriptor: DataFrame): DataFrame = {
    val blpuTable = SparkProvider.registerTempTable(blpu, "blpu")
    val organisationTable = SparkProvider.registerTempTable(organisationTable, "organisation")
    val lpiTable = SparkProvider.registerTempTable(lpi, "lpi")
    val streetTable = SparkProvider.registerTempTable(street, "street")
    val streetDescriptorTable = SparkProvider.registerTempTable(streetDescriptor, "street_descriptor")

    SparkProvider.sqlContext.sql(
      s"""SELECT blpu.uprn, org.organisation, lpi.paoText, lpi.paoStartNumber, lpi.saoText, st.streetDescriptor,
        |st.townName, st.locality
        |FROM $blpuTable
        |JOIN $organisationTable org ON blpu.uprn = org.uprn
        |JOIN $lpiTable ON blpu.uprn = lpi.uprn
        |JOIN $streetTable on lpi.usrn = street.usrn
        |JOIN $streetDescriptorTable st on street.usrn = st.usrn""".stripMargin)
  }
}
