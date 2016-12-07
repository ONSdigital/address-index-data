package uk.gov.ons.addressindex.utils

import org.apache.spark.sql.DataFrame

/**
  * Join the Csv files into single DataFrame
  */
object SqlHelper {

  def joinCsvs(blpu: DataFrame, lpi: DataFrame, organisation: DataFrame, classification: DataFrame, street: DataFrame,
               streetDescriptor: DataFrame): DataFrame = {
    blpu.registerTempTable("blpu")
    organisation.registerTempTable("organisation")
    classification.registerTempTable("classification")
    lpi.registerTempTable("lpi")
    street.registerTempTable("street")
    streetDescriptor.registerTempTable("streetDesc")

    SparkProvider.sqlContext.sql(
      """SELECT
          blpu.uprn,
          blpu.postcodeLocator,
          blpu.addressbasePostal,
          blpu.latitude,
          blpu.longitude,
          blpu.xCoordinate as easting,
          blpu.yCoordinate as northing,
          org.organisation,
          org.legalName,
          c.classificationCode,
          lpi.usrn,
          lpi.lpiKey,
          lpi.paoText,
          lpi.paoStartNumber,
          lpi.paoStartSuffix,
          lpi.paoEndNumber,
          lpi.paoEndSuffix,
          lpi.saoText,
          lpi.saoStartNumber,
          lpi.saoStartSuffix,
          lpi.saoEndNumber,
          lpi.saoEndSuffix,
          lpi.level,
          lpi.officialFlag,
          lpi.logicalStatus,
          st.streetDescriptor,
          st.townName,
          st.locality
        FROM blpu
        LEFT JOIN organisation org ON blpu.uprn = org.uprn
        LEFT JOIN classification c ON blpu.uprn = c.uprn
        LEFT JOIN lpi ON blpu.uprn = lpi.uprn
        LEFT JOIN street ON lpi.usrn = street.usrn
        LEFT JOIN streetDesc st ON street.usrn = st.usrn AND lpi.language = st.language""").na.fill("")
  }
}