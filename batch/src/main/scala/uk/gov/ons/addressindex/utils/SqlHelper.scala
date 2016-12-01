package uk.gov.ons.addressindex.utils

import org.apache.spark.sql.DataFrame

/**
  * Join the Csv files into single DataFrame
  */
object SqlHelper {

  def joinCsvs(blpu: DataFrame, lpi: DataFrame, organisation: DataFrame, street: DataFrame,
               streetDescriptor: DataFrame): DataFrame = {
    blpu.registerTempTable("blpu")
    organisation.registerTempTable("organisation")
    lpi.registerTempTable("lpi")
    street.registerTempTable("street")
    streetDescriptor.registerTempTable("streetDesc")

    SparkProvider.sqlContext.sql(
      """SELECT blpu.uprn, org.organisation, lpi.paoText, lpi.paoStartNumber, lpi.saoText, st.streetDescriptor,
        |st.townName, st.locality
        |FROM blpu
        |JOIN organisation org ON blpu.uprn = org.uprn
        |JOIN lpi ON blpu.uprn = lpi.uprn
        |JOIN street on lpi.usrn = street.usrn
        |JOIN streetDesc st on street.usrn = st.usrn""".stripMargin)
  }
}
