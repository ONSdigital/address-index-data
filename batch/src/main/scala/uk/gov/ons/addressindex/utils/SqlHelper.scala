package uk.gov.ons.addressindex.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import uk.gov.ons.addressindex.models.HybridAddressEsDocument

/**
  * Join the Csv files into single DataFrame
  */
object SqlHelper {

  def joinCsvs(blpu: DataFrame, lpi: DataFrame, organisation: DataFrame, classification: DataFrame, street: DataFrame,
               streetDescriptor: DataFrame, crossRef: DataFrame): DataFrame = {

    val blpuTable = SparkProvider.registerTempTable(blpu, "blpu")
    val organisationTable = SparkProvider.registerTempTable(organisation, "organisation")
    val classificationTable = SparkProvider.registerTempTable(classification, "classification")
    val lpiTable = SparkProvider.registerTempTable(lpi, "lpi")
    val streetTable = SparkProvider.registerTempTable(street, "street")
    val streetDescriptorTable = SparkProvider.registerTempTable(streetDescriptor, "street_descriptor")
    val crossRefTable = SparkProvider.registerTempTable(crossRef, "cross_ref")

    SparkProvider.sqlContext.sql(
      s"""SELECT
          $blpuTable.uprn,
          $blpuTable.postcodeLocator,
          $blpuTable.addressbasePostal as addressBasePostal,
          array($blpuTable.longitude, $blpuTable.latitude) as location,
          $blpuTable.xCoordinate as easting,
          $blpuTable.yCoordinate as northing,
          $blpuTable.parentUprn,
          $blpuTable.multiOccCount,
          $blpuTable.logicalStatus as blpuLogicalStatus,
          $blpuTable.localCustodianCode,
          $blpuTable.rpc,
          $organisationTable.organisation,
          $organisationTable.legalName,
          $classificationTable.classScheme,
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
          $lpiTable.logicalStatus as lpiLogicalStatus,
          $lpiTable.usrnMatchIndicator,
          $lpiTable.language,
          $streetDescriptorTable.streetDescriptor,
          $streetDescriptorTable.townName,
          $streetDescriptorTable.locality,
          $streetTable.streetClassification,
          $crossRefTable.crossReference,
          $crossRefTable.source,
          array() as relatives,
          concatNag(nvl(cast(saoStartNumber as String), ""),
                    nvl(cast(saoEndNumber as String), ""),
                    nvl(saoEndSuffix, ""),
                    nvl(saoStartSuffix, ""),
                    nvl(saoText, ""),
                    nvl(organisation, ""),
                    nvl(cast(paoStartNumber as String), ""),
                    nvl(paoStartSuffix, ""),
                    nvl(cast(paoEndNumber as String), ""),
                    nvl(paoEndSuffix, ""),
                    nvl(paoText, ""),
                    nvl(streetDescriptor, ""),
                    nvl(townName, ""),
                    nvl(locality, ""),
                    nvl(postcodeLocator)) as nagAll,
          $lpiTable.startDate as lpiStartDate,
          $lpiTable.lastUpdateDate as lpiLastUpdateDate
        FROM $blpuTable
        LEFT JOIN $organisationTable ON $blpuTable.uprn = $organisationTable.uprn
        LEFT JOIN $classificationTable ON $blpuTable.uprn = $classificationTable.uprn
        LEFT JOIN $crossRefTable ON $blpuTable.uprn = $crossRefTable.uprn
        LEFT JOIN $lpiTable ON $blpuTable.uprn = $lpiTable.uprn
        LEFT JOIN $streetTable ON $lpiTable.usrn = $streetTable.usrn
        LEFT JOIN $streetDescriptorTable ON $streetTable.usrn = $streetDescriptorTable.usrn
        AND $lpiTable.language = $streetDescriptorTable.language""").na.fill("")
  }

  /**
    * Constructs a hybrid index from nag and paf dataframes
    * We couldn't use Spark Sql because it does not contain `collect_list` until 2.0
    * Hive does not support aggregating complex types in the `collect_list` udf
    */
  def aggregateHybridIndex(paf: DataFrame, nag: DataFrame): RDD[HybridAddressEsDocument] = {

    val pafGroupedRdd = paf.rdd.keyBy(t => t.getLong(3))
    val nagGroupedRdd = nag.rdd.keyBy(t => t.getLong(0))

    // Following line will group rows in 2 groups: lpi and paf
    // The first element in each new row will contain `uprn` as the first key
    val groupedRdd = nagGroupedRdd.cogroup(pafGroupedRdd)

    groupedRdd.map {
      case (uprn, (lpiArray, pafArray)) =>
        val lpis = lpiArray.toSeq.map(HybridAddressEsDocument.rowToLpi)
        val pafs = pafArray.toSeq.map(HybridAddressEsDocument.rowToPaf)

        val lpiPostCode: Option[String] = lpis.headOption.flatMap(_.get("postcodeLocator").map(_.toString))
        val pafPostCode: Option[String] = pafs.headOption.flatMap(_.get("postcode").map(_.toString))

        val postCode = if (pafPostCode.isDefined) pafPostCode.getOrElse("")
        else lpiPostCode.getOrElse("")

        val splitPostCode = postCode.split(" ")
        val (postCodeOut, postCodeIn) =
          if (splitPostCode.size == 2 && splitPostCode(1).length == 3) (splitPostCode(0), splitPostCode(1))
          else ("", "")


        HybridAddressEsDocument(
          uprn,
          postCodeIn,
          postCodeOut,
          lpis,
          pafs
        )
    }
  }
}
