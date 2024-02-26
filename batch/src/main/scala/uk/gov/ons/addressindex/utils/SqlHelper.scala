package uk.gov.ons.addressindex.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{ArrayType, FloatType, LongType}
import uk.gov.ons.addressindex.models.{HybridAddressEsDocument, HybridAddressSkinnyEsDocument}
import uk.gov.ons.addressindex.readers.AddressIndexFileReader

import scala.util.Try

/**
  * Join the Csv files into single DataFrame
  */
object SqlHelper {

  def joinCsvs(blpu: DataFrame, classification: DataFrame, lpi: DataFrame, organisation: DataFrame, street: DataFrame,
               streetDescriptor: DataFrame, historical: Boolean = true, skinny: Boolean = false): DataFrame = {

    val classificationTable = SparkProvider.registerTempTable(classification, "classification")
    val blpuTable =
      if (historical) {
        val blpuWithHistory = SparkProvider.registerTempTable(blpu, "blpuWithHistory")
        val blpuWithHistoryDF =
          if (skinny)
            SparkProvider.sparkContext.sql(s"""SELECT b.*, c.classificationCode
              FROM $blpuWithHistory b
                   LEFT JOIN $classificationTable c ON b.uprn = c.uprn
              WHERE NOT (b.addressBasePostal = 'N' AND NOT c.classificationCode LIKE 'R%')""")
          else
            SparkProvider.sparkContext.sql(s"""SELECT b.* FROM $blpuWithHistory b""")
        SparkProvider.registerTempTable(blpuWithHistoryDF, "blpu")
      } else {
        val blpuNoHistory = SparkProvider.registerTempTable(blpu, "blpuNoHistory")
        val blpuNoHistoryDF =
          if (skinny)
            SparkProvider.sparkContext.sql(s"""SELECT b.*, c.classificationCode
              FROM $blpuNoHistory b
                   LEFT JOIN $classificationTable c ON b.uprn = c.uprn
              WHERE b.logicalStatus != 8 AND c.classificationCode !='DUMMY' AND NOT (b.addressBasePostal = 'N' AND NOT c.classificationCode LIKE 'R%')""")
          else
            SparkProvider.sparkContext.sql(s"""SELECT b.*, c.classificationCode
              FROM $blpuNoHistory b
                   LEFT OUTER JOIN $classificationTable c ON b.uprn = c.uprn
              WHERE b.logicalStatus != 8 AND c.classificationCode !='DUMMY' """)
        SparkProvider.registerTempTable(blpuNoHistoryDF, "blpu")
      }
    val organisationTable = SparkProvider.registerTempTable(organisation, "organisation")
    val lpiTable =
      if (historical) {
        SparkProvider.registerTempTable(lpi, "lpi")
      } else {
        val lpiNoHistory = SparkProvider.registerTempTable(lpi, "lpiNoHistory")
        val lpiNoHistoryDF = SparkProvider.sparkContext.sql(s"""SELECT l.* FROM $lpiNoHistory l WHERE l.logicalStatus != 8 """)
        SparkProvider.registerTempTable(lpiNoHistoryDF, "lpi")
      }
    val streetTable = SparkProvider.registerTempTable(street, "street")
    val streetDescriptorTable = SparkProvider.registerTempTable(streetDescriptor, "street_descriptor")

    SparkProvider.sparkContext.sql(
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
        $lpiTable.startDate as lpiStartDate,
        $lpiTable.lastUpdateDate as lpiLastUpdateDate,
        $lpiTable.endDate as lpiEndDate,
        $blpuTable.country
      FROM $blpuTable
      LEFT JOIN $organisationTable ON $blpuTable.uprn = $organisationTable.uprn
      LEFT JOIN $lpiTable ON $blpuTable.uprn = $lpiTable.uprn
      LEFT JOIN $streetTable ON $lpiTable.usrn = $streetTable.usrn
      LEFT JOIN $streetDescriptorTable ON $streetTable.usrn = $streetDescriptorTable.usrn
      AND $lpiTable.language = $streetDescriptorTable.language""").na.fill("")
  }

  /**
    * Aggregates data forming lists of siblings and their parents per level of the hierarchy
    * (grouped by root uprn)
    *
    * @param hierarchy hierarchy data
    * @return dataframe containing layers/levels of hierarchy
    */
  def aggregateHierarchyInformation(hierarchy: DataFrame): DataFrame = {
    val hierarchyTable = SparkProvider.registerTempTable(hierarchy, "hierarchy")

    SparkProvider.sparkContext.sql(
      s"""SELECT
            primaryUprn,
            thisLayer as level,
            collect_list(uprn) as siblings,
            collect_list(parentUprn) as parents
          FROM
            $hierarchyTable
          GROUP BY primaryUprn, thisLayer
       """
    )
  }

  def aggregateCrossRefInformation(crossRef: DataFrame): DataFrame = {
    val crossRefTable = SparkProvider.registerTempTable(crossRef, "crossRef")

    SparkProvider.sparkContext.sql(
      s"""SELECT
            uprn,
            crossReference,
            source
          FROM
            $crossRefTable
          GROUP BY uprn, crossReference, source
       """
    )
  }

  def aggregateRDMFInformation(rdmf: DataFrame): DataFrame = {
    val rdmfTable = SparkProvider.registerTempTable(rdmf, "rdmf")

    SparkProvider.sparkContext.sql(
      s"""SELECT
            uprn,
            address_entry_id,
            address_entry_id_alphanumeric_backup
          FROM
            $rdmfTable
          GROUP BY uprn, address_entry_id, address_entry_id_alphanumeric_backup
       """
    )
  }

  def aggregateClassificationsInformation(classifications: DataFrame): DataFrame = {
    val classificationsTable = SparkProvider.registerTempTable(classifications, "classifications")

    SparkProvider.sparkContext.sql(
      s"""SELECT
            uprn,
            classificationCode
          FROM
            $classificationsTable
          WHERE
            classScheme = 'AddressBase Premium Classification Scheme'
          GROUP BY uprn, classificationCode
       """
    )
  }

  private def createPafGrouped(paf: DataFrame, nag: DataFrame, historical: Boolean) : DataFrame =
    if (historical) paf.groupBy("uprn").agg(functions.collect_list(functions.struct("*")).as("paf"))
    else paf.join(nag, Seq("uprn"), joinType = "leftsemi")
      .select("recordIdentifier", "changeType", "proOrder", "uprn", "udprn", "organisationName", "departmentName",
        "subBuildingName", "buildingName", "buildingNumber", "dependentThoroughfare", "thoroughfare",
        "doubleDependentLocality", "dependentLocality", "postTown", "postcode", "postcodeType", "deliveryPointSuffix",
        "welshDependentThoroughfare", "welshThoroughfare", "welshDoubleDependentLocality", "welshDependentLocality",
        "welshPostTown", "poBoxNumber", "processDate", "startDate", "endDate", "lastUpdateDate", "entryDate")
      .groupBy("uprn").agg(functions.collect_list(functions.struct("*")).as("paf"))

  private def createPafNagGrouped(nag: DataFrame, pafGrouped: DataFrame) : DataFrame =
    nag.groupBy("uprn")
      .agg(functions.collect_list(functions.struct("*")).as("lpis"))
      .join(pafGrouped, Seq("uprn"), "left_outer")

  /**
    * Constructs a hybrid index from nag and paf dataframes
    */
  def aggregateHybridSkinnyIndex(paf: DataFrame, nag: DataFrame, historical: Boolean = true, nisraAddress1YearAgo: Boolean = false): RDD[HybridAddressSkinnyEsDocument] = {

    val rdmfGrouped =  aggregateRDMFInformation(AddressIndexFileReader.readRDMFCSV())
      .groupBy("uprn")
      .agg(functions.collect_list(functions.struct("address_entry_id","address_entry_id_alphanumeric_backup")).as("entryids"))

    val crossRefGrouped = aggregateCrossRefInformation(AddressIndexFileReader.readCrossrefCSV())
      .groupBy("uprn")
      .agg(functions.collect_list(functions.struct("crossReference", "source")).as("crossRefs"))

    val classificationsGrouped = aggregateClassificationsInformation(AddressIndexFileReader.readClassificationCSV())
      .groupBy("uprn")
      .agg(functions.collect_list(functions.struct("classificationCode")).as("classification"))

    val hierarchyDF = AddressIndexFileReader.readHierarchyCSV()

    val hierarchyGrouped = aggregateHierarchyInformation(hierarchyDF)
      .groupBy("primaryUprn")
      .agg(functions.collect_list(functions.struct("level", "siblings", "parents")).as("relatives"))

    val hierarchyJoined = hierarchyDF
      .join(hierarchyGrouped, Seq("primaryUprn"), "left_outer")
      .select("uprn", "parentUprn","addressType","estabType")

    // If non-historical there could be zero lpis associated with the PAF record since historical lpis were filtered
    // out at the joinCsvs stage. These need to be removed.
    val pafGrouped = createPafGrouped(paf, nag, historical)

    // DataFrame of paf and lpis by uprn
    val pafNagGrouped = createPafNagGrouped(nag, pafGrouped)

    val pafNagHierGrouped = pafNagGrouped
      .join(crossRefGrouped, Seq("uprn"), "left_outer")
      .join(hierarchyJoined, Seq("uprn"), "left_outer")
      .join(classificationsGrouped, Seq("uprn"), "left_outer")
      .join(rdmfGrouped, Seq("uprn"), "left_outer")

    pafNagHierGrouped.rdd.map {
      row =>
        val uprn = row.getAs[Long]("uprn")
        val paf = Option(row.getAs[Seq[Row]]("paf")).getOrElse(Seq())
        val lpis = Option(row.getAs[Seq[Row]]("lpis")).getOrElse(Seq())
        val parentUprn = Option(row.getAs[Long]("parentUprn"))
        val classifications = Option(row.getAs[Seq[Row]]("classification")).getOrElse(Seq())
        val crossRefs = Option(row.getAs[Seq[Row]]("crossRefs")).getOrElse(Seq())
        val outputCrossRefs = crossRefs.map(row => HybridAddressEsDocument.rowToCrossRef(row))
        val outputLpis = lpis.map(row => HybridAddressSkinnyEsDocument.rowToLpi(row))
        val outputPaf = paf.map(row => HybridAddressSkinnyEsDocument.rowToPaf(row))
        val classificationCode: Option[String] = classifications.map(row => row.getAs[String]("classificationCode")).headOption
        val addressType = Option(row.getAs[String]("addressType")).getOrElse("")
        val estabType = Option(row.getAs[String]("estabType")).getOrElse("")
        val isCouncilTax:Boolean = outputCrossRefs.mkString.contains("7666VC")
        val isNonDomesticRate:Boolean = outputCrossRefs.mkString.contains("7666VN")

        val lpiPostCode: Option[String] = outputLpis.headOption.flatMap(_.get("postcodeLocator").map(_.toString))
        val pafPostCode: Option[String] = outputPaf.headOption.flatMap(_.get("postcode").map(_.toString))

        val postCode = if (pafPostCode.isDefined) pafPostCode.getOrElse("")
        else lpiPostCode.getOrElse("")

        val fromSource = "EW"

        val countryCode = outputLpis.headOption.flatMap(_.get("country").map(_.toString)).getOrElse("E")

        val englishNag = outputLpis.find(_.getOrElse("language", "ENG") == "ENG")

        val lpiStreet: Option[String] = outputLpis.headOption.flatMap(_.get("streetDescriptor").map(_.toString))
        val lpiStreetEng: Option[String] = englishNag.map(_.get("streetDescriptor").map(_.toString).getOrElse(""))
        val pafStreet: Option[String] = outputPaf.headOption.flatMap(_.get("thoroughfare").map(_.toString))
        val lpiTown: Option[String] = outputLpis.headOption.flatMap(_.get("townName").map(_.toString))
        val lpiTownEng: Option[String] = englishNag.map(_.get("townName").map(_.toString).getOrElse(""))
        val lpiLocality: Option[String] = outputLpis.headOption.flatMap(_.get("locality").map(_.toString))
        val lpiLocalityEng: Option[String] = englishNag.map(_.get("locality").map(_.toString).getOrElse(""))
        val pafTown: Option[String] = outputPaf.headOption.flatMap(_.get("postTown").map(_.toString))
        val pafDepend = outputPaf.headOption.flatMap(_.get("dependentLocality").map(_.toString))

        val lpiStartEng: Option[String] = englishNag.map(_.get("mixedNagStart").map(_.toString).getOrElse(""))
        val lpiStart: Option[String] = outputLpis.headOption.flatMap(_.get("mixedNagStart").map(_.toString))
        val bestStreet: String = if (pafStreet.getOrElse("").nonEmpty) pafStreet.getOrElse("")
        else if (lpiStreetEng.getOrElse("").nonEmpty) lpiStreetEng.getOrElse("")
        else if (lpiStreet.getOrElse("").nonEmpty) lpiStreet.getOrElse("")
        else if (lpiStartEng.getOrElse("").nonEmpty) "(" + lpiStartEng.getOrElse("") + ")"
        else "(" + lpiStart.getOrElse("") + ")"

        val bestTown: String = if (pafDepend.getOrElse("").nonEmpty) pafDepend.getOrElse("")
        else if (lpiLocalityEng.getOrElse("").nonEmpty) lpiLocalityEng.getOrElse("")
        else if (lpiLocality.getOrElse("").nonEmpty) lpiLocality.getOrElse("")
        else if (lpiTownEng.getOrElse("").nonEmpty) lpiTownEng.getOrElse("")
        else if (lpiTown.getOrElse("").nonEmpty) lpiTown.getOrElse("")
        else pafTown.getOrElse("")

        val postcodeStreetTown = (postCode + "_" + bestStreet + "_" + bestTown).replace(".","").replace("'","")
        val postTown = pafTown.orNull

        val mixedKeys = List("mixedNag", "mixedWelshNag", "mixedPaf", "mixedWelshPaf")
        val mixedPartial = (outputLpis ++ outputPaf).flatMap( mixedKeys collect _ )
        val mixedPartialTokens = mixedPartial.flatMap(_.toString.split(",").filter(_.nonEmpty)).distinct.mkString(",")
        val mixedPartialTokensExtraDedup = mixedPartialTokens.replaceAll(","," ").split(" ").distinct.mkString(" ").replaceAll("  "," ")

        val entryIds = Option(row.getAs[Seq[Row]]("entryids")).getOrElse(Seq())
        val addressEntryId: Option[Long] = entryIds.map(row => row.getAs[Long]("address_entry_id")).headOption
        val addressEntryIdAlphanumericBackup: Option[String] = entryIds.map(row => row.getAs[String]("address_entry_id_alphanumeric_backup")).headOption

        HybridAddressSkinnyEsDocument(
          uprn,
          parentUprn.getOrElse(0L),
          outputLpis,
          outputPaf,
          classificationCode,
          postCode,
          fromSource,
          countryCode,
          postcodeStreetTown,
          postTown,
          mixedPartialTokensExtraDedup,
          addressEntryId,
          addressEntryIdAlphanumericBackup
        )
    }
  }

    /**
    * Constructs a hybrid index from nag and paf dataframes
    */
  def aggregateHybridIndex(paf: DataFrame, nag: DataFrame, historical: Boolean = true): RDD[HybridAddressEsDocument] = {

    val rdmfGrouped =  aggregateRDMFInformation(AddressIndexFileReader.readRDMFCSV())
      .groupBy("uprn")
      .agg(functions.collect_list(functions.struct("address_entry_id","address_entry_id_alphanumeric_backup")).as("entryids"))

    val crossRefGrouped = aggregateCrossRefInformation(AddressIndexFileReader.readCrossrefCSV())
      .groupBy("uprn")
      .agg(functions.collect_list(functions.struct("crossReference", "source")).as("crossRefs"))

    val classificationsGrouped = aggregateClassificationsInformation(AddressIndexFileReader.readClassificationCSV())
      .groupBy("uprn")
      .agg(functions.collect_list(functions.struct("classificationCode")).as("classification"))

    val hierarchyDF = AddressIndexFileReader.readHierarchyCSV()

    val hierarchyGrouped = aggregateHierarchyInformation(hierarchyDF)
      .groupBy("primaryUprn")
      .agg(functions.collect_list(functions.struct("level", "siblings", "parents")).as("relatives"))

    val hierarchyJoinedWithRelatives = hierarchyDF
      .join(hierarchyGrouped, Seq("primaryUprn"), "left_outer")
      .select("uprn", "parentUprn", "relatives","addressType","estabType")

    // If non-historical there could be zero lpis associated with the PAF record since historical lpis were filtered
    // out at the joinCsvs stage. These need to be removed.
    val pafGrouped = createPafGrouped(paf, nag, historical)

    // DataFrame of paf and lpis by uprn
    val pafNagGrouped = createPafNagGrouped(nag, pafGrouped)

    val pafNagCrossHierGrouped = pafNagGrouped
      .join(crossRefGrouped, Seq("uprn"), "left_outer")
      .join(hierarchyJoinedWithRelatives, Seq("uprn"), "left_outer")
      .join(classificationsGrouped, Seq("uprn"), "left_outer")
      .join(rdmfGrouped, Seq("uprn"), "left_outer")

    pafNagCrossHierGrouped.rdd.map {
      row =>
        val uprn = row.getAs[Long]("uprn")
        val paf = Option(row.getAs[Seq[Row]]("paf")).getOrElse(Seq())
        val lpis = Option(row.getAs[Seq[Row]]("lpis")).getOrElse(Seq())
        val crossRefs = Option(row.getAs[Seq[Row]]("crossRefs")).getOrElse(Seq())
        val relatives = Option(row.getAs[Seq[Row]]("relatives")).getOrElse(Seq())
        val parentUprn = Option(row.getAs[Long]("parentUprn"))
        val classifications = Option(row.getAs[Seq[Row]]("classification")).getOrElse(Seq())
        val addressType = Option(row.getAs[String]("addressType")).getOrElse("")
        val estabType = Option(row.getAs[String]("estabType")).getOrElse("")
        val outputLpis = lpis.map(row => HybridAddressEsDocument.rowToLpi(row))
        val outputPaf = paf.map(row => HybridAddressEsDocument.rowToPaf(row))
        val outputCrossRefs = crossRefs.map(row => HybridAddressEsDocument.rowToCrossRef(row))
        val outputRelatives = relatives.map(row => HybridAddressEsDocument.rowToHierarchy(row))
        val classificationCode: Option[String] = classifications.map(row => row.getAs[String]("classificationCode")).headOption

        val isCouncilTax:Boolean = outputCrossRefs.mkString.contains("7666VC")
        val isNonDomesticRate:Boolean = outputCrossRefs.mkString.contains("7666VN")

        val lpiPostCode: Option[String] = outputLpis.headOption.flatMap(_.get("postcodeLocator").map(_.toString))
        val pafPostCode: Option[String] = outputPaf.headOption.flatMap(_.get("postcode").map(_.toString))

        val postCode = if (pafPostCode.isDefined) pafPostCode.getOrElse("")
        else lpiPostCode.getOrElse("")

        val splitPostCode = postCode.split(" ")
        val (postCodeOut, postCodeIn) =
          if (splitPostCode.size == 2 && splitPostCode(1).length == 3) (splitPostCode(0), splitPostCode(1))
          else ("", "")

        val fromSource = "EW"

        val countryCode = outputLpis.headOption.flatMap(_.get("country").map(_.toString)).getOrElse("E")

        val englishNag = outputLpis.find(_.getOrElse("language", "ENG") == "ENG")

        val lpiStreet: Option[String] = outputLpis.headOption.flatMap(_.get("streetDescriptor").map(_.toString))
        val lpiStreetEng: Option[String] = englishNag.map(_.get("streetDescriptor").map(_.toString).getOrElse(""))
        val pafStreet: Option[String] = outputPaf.headOption.flatMap(_.get("thoroughfare").map(_.toString))
        val lpiTown: Option[String] = outputLpis.headOption.flatMap(_.get("townName").map(_.toString))
        val lpiTownEng: Option[String] = englishNag.map(_.get("townName").map(_.toString).getOrElse(""))
        val lpiLocality: Option[String] = outputLpis.headOption.flatMap(_.get("locality").map(_.toString))
        val lpiLocalityEng: Option[String] = englishNag.map(_.get("locality").map(_.toString).getOrElse(""))
        val pafTown: Option[String] = outputPaf.headOption.flatMap(_.get("postTown").map(_.toString))
        val pafDepend = outputPaf.headOption.flatMap(_.get("dependentLocality").map(_.toString))

        val lpiStartEng: Option[String] = englishNag.map(_.get("mixedNagStart").map(_.toString).getOrElse(""))
        val lpiStart: Option[String] = outputLpis.headOption.flatMap(_.get("mixedNagStart").map(_.toString))
        val bestStreet: String = if (pafStreet.getOrElse("").nonEmpty) pafStreet.getOrElse("")
        else if (lpiStreetEng.getOrElse("").nonEmpty) lpiStreetEng.getOrElse("")
        else if (lpiStreet.getOrElse("").nonEmpty) lpiStreet.getOrElse("")
        else if (lpiStartEng.getOrElse("").nonEmpty) "(" + lpiStartEng.getOrElse("") + ")"
        else "(" + lpiStart.getOrElse("") + ")"

        val bestTown: String = if (pafDepend.getOrElse("").nonEmpty) pafDepend.getOrElse("")
        else if (lpiLocalityEng.getOrElse("").nonEmpty) lpiLocalityEng.getOrElse("")
        else if (lpiLocality.getOrElse("").nonEmpty) lpiLocality.getOrElse("")
        else if (lpiTownEng.getOrElse("").nonEmpty) lpiTownEng.getOrElse("")
        else if (lpiTown.getOrElse("").nonEmpty) lpiTown.getOrElse("")
        else pafTown.getOrElse("")

        val postcodeStreetTown = (postCode + "_" + bestStreet + "_" + bestTown).replace(".","").replace("'","")
        val postTown = pafTown.orNull

        val mixedKeys = List("mixedNag", "mixedWelshNag", "mixedPaf", "mixedWelshPaf")
        val mixedPartial = (outputLpis ++ outputPaf).flatMap( mixedKeys collect _ )
        val mixedPartialTokens = mixedPartial.flatMap(_.toString.split(",").filter(_.nonEmpty)).distinct.mkString(",")
        val mixedPartialTokensExtraDedup = mixedPartialTokens.replaceAll(","," ").split(" ").distinct.mkString(" ").replaceAll("  "," ")

        val entryIds = Option(row.getAs[Seq[Row]]("entryids")).getOrElse(Seq())
        val addressEntryId: Option[Long] = entryIds.map(row => row.getAs[Long]("address_entry_id")).headOption
        val addressEntryIdAlphanumericBackup: Option[String] = entryIds.map(row => row.getAs[String]("address_entry_id_alphanumeric_backup")).headOption

        HybridAddressEsDocument(
          uprn,
          postCodeIn,
          postCodeOut,
          parentUprn.getOrElse(0L),
          outputRelatives,
          outputLpis,
          outputPaf,
          outputCrossRefs,
          classificationCode,
          postCode,
          fromSource,
          countryCode,
          postcodeStreetTown,
          postTown,
          mixedPartialTokensExtraDedup,
          addressEntryId,
          addressEntryIdAlphanumericBackup
        )
    }
  }


}