package uk.gov.ons.addressindex.utils

import org.apache.spark.sql.DataFrame

/**
  * Join the Csv files into single DataFrame
  */
object SqlHelper {

  def joinCsvs(blpu: DataFrame, lpi: DataFrame, organisation: DataFrame, classification: DataFrame, street: DataFrame,
               streetDescriptor: DataFrame): DataFrame = {

    val blpuTable = SparkProvider.registerTempTable(blpu, "blpu")
    val organisationTable = SparkProvider.registerTempTable(organisation, "organisation")
    val classificationTable = SparkProvider.registerTempTable(classification, "classification")
    val lpiTable = SparkProvider.registerTempTable(lpi, "lpi")
    val streetTable = SparkProvider.registerTempTable(street, "street")
    val streetDescriptorTable = SparkProvider.registerTempTable(streetDescriptor, "street_descriptor")

    SparkProvider.sqlContext.sql(
      s"""SELECT
          $blpuTable.uprn,
          $blpuTable.postcodeLocator,
          $blpuTable.addressbasePostal,
          $blpuTable.latitude,
          $blpuTable.longitude,
          $blpuTable.xCoordinate as easting,
          $blpuTable.yCoordinate as northing,
          $organisationTable.organisation,
          $organisationTable.legalName,
          $classificationTable.classificationCode,
          $lpiTable.usrn,
          $lpiTable.lpiKey,
          $lpiTable.paoText,
          $lpiTable.paoStartNumber,
          $lpiTable.paoStartSuffix,
          $lpiTable.paoEndNumber,
          $lpiTable.paoEndSuffix,
          $lpiTable.saoText,
          $lpiTable.saoStartNumber,
          $lpiTable.saoStartSuffix,
          $lpiTable.saoEndNumber,
          $lpiTable.saoEndSuffix,
          $lpiTable.level,
          $lpiTable.officialFlag,
          $lpiTable.logicalStatus,
          $streetDescriptorTable.streetDescriptor,
          $streetDescriptorTable.townName,
          $streetDescriptorTable.locality
        FROM $blpuTable
        LEFT JOIN $organisationTable ON $blpuTable.uprn = $organisationTable.uprn
        LEFT JOIN $classificationTable ON $blpuTable.uprn = $classificationTable.uprn
        LEFT JOIN $lpiTable ON $blpuTable.uprn = $lpiTable.uprn
        LEFT JOIN $streetTable ON $lpiTable.usrn = $streetTable.usrn
        LEFT JOIN $streetDescriptorTable ON $streetTable.usrn = $streetDescriptorTable.usrn
        AND $lpiTable.language = $streetDescriptorTable.language""").na.fill("")
  }
}
