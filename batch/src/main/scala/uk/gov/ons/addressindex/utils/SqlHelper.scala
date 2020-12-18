package uk.gov.ons.addressindex.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{ArrayType, FloatType, LongType}
import org.apache.spark.sql.functions.{col, when}
import uk.gov.ons.addressindex.models.{HybridAddressEsDocument, HybridAddressNisraEsDocument, HybridAddressSkinnyEsDocument, HybridAddressSkinnyNisraEsDocument}
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
            SparkProvider.sqlContext.sql(s"""SELECT b.*, c.classificationCode
              FROM $blpuWithHistory b
                   LEFT JOIN $classificationTable c ON b.uprn = c.uprn
              WHERE NOT (b.addressBasePostal = 'N' AND NOT c.classificationCode LIKE 'R%')""")
          else
            SparkProvider.sqlContext.sql(s"""SELECT b.* FROM $blpuWithHistory b""")
        SparkProvider.registerTempTable(blpuWithHistoryDF, "blpu")
      } else {
        val blpuNoHistory = SparkProvider.registerTempTable(blpu, "blpuNoHistory")
        val blpuNoHistoryDF =
          if (skinny)
            SparkProvider.sqlContext.sql(s"""SELECT b.*, c.classificationCode
              FROM $blpuNoHistory b
                   LEFT JOIN $classificationTable c ON b.uprn = c.uprn
              WHERE b.logicalStatus != 8 AND NOT (b.addressBasePostal = 'N' AND NOT c.classificationCode LIKE 'R%')""")
          else
            SparkProvider.sqlContext.sql(s"""SELECT b.* FROM $blpuNoHistory b WHERE b.logicalStatus != 8""")
        SparkProvider.registerTempTable(blpuNoHistoryDF, "blpu")
      }
    val organisationTable = SparkProvider.registerTempTable(organisation, "organisation")
    val lpiTable =
      if (historical) {
        SparkProvider.registerTempTable(lpi, "lpi")
      } else {
        val lpiNoHistory = SparkProvider.registerTempTable(lpi, "lpiNoHistory")
        val lpiNoHistoryDF = SparkProvider.sqlContext.sql(s"""SELECT l.* FROM $lpiNoHistory l WHERE l.logicalStatus != 8 """)
        SparkProvider.registerTempTable(lpiNoHistoryDF, "lpi")
      }
    val streetTable = SparkProvider.registerTempTable(street, "street")
    val streetDescriptorTable = SparkProvider.registerTempTable(streetDescriptor, "street_descriptor")

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

    SparkProvider.sqlContext.sql(
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

    SparkProvider.sqlContext.sql(
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

  def aggregateClassificationsInformation(classifications: DataFrame): DataFrame = {
    val classificationsTable = SparkProvider.registerTempTable(classifications, "classifications")

    SparkProvider.sqlContext.sql(
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

  def nisraData(nisra: DataFrame, nisraAddress1YearAgo: Boolean = false): DataFrame = {

    val filteredNisraDF = if (nisraAddress1YearAgo) nisra.filter("address1YearAgo != 'New'")
                          else nisra.filter("address1YearAgo != 'Old'")

    filteredNisraDF.select(filteredNisraDF("uprn").cast(LongType),
        functions.regexp_replace(filteredNisraDF("subBuildingName"), "NULL", "").as("subBuildingName"),
        functions.regexp_replace(filteredNisraDF("buildingName"), "NULL", "").as("buildingName"),
        functions.regexp_replace(filteredNisraDF("buildingNumber"), "NULL", "").as("buildingNumber"),
        functions.regexp_replace(filteredNisraDF("paoStartNumber"), "NULL", "").as("paoStartNumber"),
        functions.regexp_replace(filteredNisraDF("paoEndNumber"), "NULL", "").as("paoEndNumber"),
        functions.regexp_replace(filteredNisraDF("paoStartSuffix"), "NULL", "").as("paoStartSuffix"),
        functions.regexp_replace(filteredNisraDF("paoEndSuffix"), "NULL", "").as("paoEndSuffix"),
        functions.regexp_replace(filteredNisraDF("paoText"), "NULL", "").as("paoText"),
        functions.regexp_replace(filteredNisraDF("saoStartNumber"), "NULL", "").as("saoStartNumber"),
        functions.regexp_replace(filteredNisraDF("saoEndNumber"), "NULL", "").as("saoEndNumber"),
        functions.regexp_replace(filteredNisraDF("saoStartSuffix"), "NULL", "").as("saoStartSuffix"),
        functions.regexp_replace(filteredNisraDF("saoEndSuffix"), "NULL", "").as("saoEndSuffix"),
        functions.regexp_replace(filteredNisraDF("saoText"), "NULL", "").as("saoText"),
        functions.regexp_replace(filteredNisraDF("organisationName"), "NULL", "").as("organisationName"),
        functions.regexp_replace(filteredNisraDF("thoroughfare"), "NULL", "").as("thoroughfare"),
        functions.regexp_replace(filteredNisraDF("altThoroughfare"), "NULL", "").as("altThoroughfare"),
        functions.regexp_replace(filteredNisraDF("dependentThoroughfare"), "NULL", "").as("dependentThoroughfare"),
        functions.regexp_replace(filteredNisraDF("locality"), "NULL", "").as("locality"),
        filteredNisraDF("udprn"),
        functions.regexp_replace(filteredNisraDF("townName"), "NULL", "").as("townName"),
        functions.regexp_replace(filteredNisraDF("postcode"), "NULL", "").as("postcode"),
        filteredNisraDF("xCoordinate").as("easting").cast(FloatType),
        filteredNisraDF("yCoordinate").as("northing").cast(FloatType),
        functions.array(filteredNisraDF("longitude"),filteredNisraDF("latitude"))
          .as("location").cast(ArrayType(FloatType)),
        functions.to_date(filteredNisraDF("creationDate"), "yyyy-MM-dd").as("creationDate"),
        functions.to_date(filteredNisraDF("commencementDate"), "yyyy-MM-dd").as("commencementDate"),
        functions.to_date(filteredNisraDF("archivedDate"), "yyyy-MM-dd").as("archivedDate"),
        functions.regexp_replace(filteredNisraDF("classificationCode"), "NULL", "").as("classificationCode"),
        functions.regexp_replace(filteredNisraDF("townland"), "NULL", "").as("townland"),
        functions.regexp_replace(filteredNisraDF("county"), "NULL", "").as("county"),
        functions.regexp_replace(filteredNisraDF("localCustodianCode"), "NULL", "").as("localCustodianCode"),
        filteredNisraDF("blpuState"),
        filteredNisraDF("logicalStatus"),
        functions.regexp_replace(filteredNisraDF("addressType"), "NULL", "").as("addressType"),
        functions.regexp_replace(filteredNisraDF("estabType"), "NULL", "").as("estabType"),
        functions.regexp_replace(filteredNisraDF("lad"), "NULL", "").as("lad"),
        functions.regexp_replace(filteredNisraDF("region"), "NULL", "").as("region"),
        filteredNisraDF("recordIdentifier"),
        filteredNisraDF("parentUprn"),
        filteredNisraDF("usrn"),
        filteredNisraDF("primaryUprn"),
        functions.regexp_replace(filteredNisraDF("secondaryUprn"), "NULL", "").as("secondaryUprn"),
        filteredNisraDF("thisLayer"),
        filteredNisraDF("layers"),
        functions.regexp_replace(filteredNisraDF("nodeType"), "NULL", "").as("nodeType"),
        functions.regexp_replace(filteredNisraDF("addressLine1"), "NULL", "").as("addressLine1"),
        functions.regexp_replace(filteredNisraDF("addressLine2"), "NULL", "").as("addressLine2"),
        functions.regexp_replace(filteredNisraDF("addressLine3"), "NULL", "").as("addressLine3"),
        functions.regexp_replace(filteredNisraDF("tempCoords"), "NULL", "").as("tempCoords"),
        functions.regexp_replace(filteredNisraDF("address1YearAgo"), "NULL", "").as("address1YearAgo"),
        functions.regexp_replace(filteredNisraDF("postTown"), "NULL", "").as("postTown")
      )
  }

  private val crossRefGrouped = aggregateCrossRefInformation(AddressIndexFileReader.readCrossrefCSV())
    .groupBy("uprn")
    .agg(functions.collect_list(functions.struct("crossReference", "source")).as("crossRefs"))

  private val classificationsGrouped = aggregateClassificationsInformation(AddressIndexFileReader.readClassificationCSV())
    .groupBy("uprn")
    .agg(functions.collect_list(functions.struct("classificationCode")).as("classification"))

  private val hierarchyDF = AddressIndexFileReader.readHierarchyCSV()

  private val hierarchyGrouped = aggregateHierarchyInformation(hierarchyDF)
    .groupBy("primaryUprn")
    .agg(functions.collect_list(functions.struct("level", "siblings", "parents")).as("relatives"))

  private val hierarchyJoined = hierarchyDF
    .join(hierarchyGrouped, Seq("primaryUprn"), "left_outer")
    .select("uprn", "parentUprn","addressType","estabType")

  private val hierarchyJoinedWithRelatives = hierarchyDF
    .join(hierarchyGrouped, Seq("primaryUprn"), "left_outer")
    .select("uprn", "parentUprn", "relatives","addressType","estabType")

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

  private def createNisraGrouped(nisra: DataFrame, nisraAddress1YearAgo: Boolean) =
    nisraData(nisra, nisraAddress1YearAgo)
      .groupBy("uprn")
      .agg(functions.collect_list(functions.struct("*")).as("nisra"))

  private def createPafNagHierGrouped(pafNagGrouped: DataFrame) : DataFrame = pafNagGrouped
    .join(crossRefGrouped, Seq("uprn"), "left_outer")
    .join(hierarchyJoined, Seq("uprn"), "left_outer")
    .join(classificationsGrouped, Seq("uprn"), "left_outer")

  private def createPafNagHierGroupedWithRelatives(pafNagGrouped: DataFrame) : DataFrame = pafNagGrouped
    .join(crossRefGrouped, Seq("uprn"), "left_outer")
    .join(hierarchyJoinedWithRelatives, Seq("uprn"), "left_outer")
    .join(classificationsGrouped, Seq("uprn"), "left_outer")

  /**
    * Constructs a hybrid index from nag and paf dataframes
    */
  def aggregateHybridSkinnyNisraIndex(paf: DataFrame, nag: DataFrame, nisra: DataFrame, historical: Boolean = true, nisraAddress1YearAgo: Boolean = false): RDD[HybridAddressSkinnyNisraEsDocument] = {

    // If non-historical there could be zero lpis associated with the PAF record since historical lpis were filtered
    // out at the joinCsvs stage. These need to be removed.
    val pafGrouped = createPafGrouped(paf, nag, historical)

    // DataFrame of nisra by uprn
    val nisraGrouped = createNisraGrouped(nisra, nisraAddress1YearAgo)

    // DataFrame of paf and lpis by uprn joined with nisra
    val pafNagGrouped = createPafNagGrouped(nag, pafGrouped)
      .join(nisraGrouped, Seq("uprn"), "full_outer")

    val pafNagHierGrouped = createPafNagHierGrouped(pafNagGrouped)

    pafNagHierGrouped.rdd.map {
      row =>
        val uprn = row.getAs[Long]("uprn")
        val paf = Option(row.getAs[Seq[Row]]("paf")).getOrElse(Seq())
        val lpis = Option(row.getAs[Seq[Row]]("lpis")).getOrElse(Seq())
        val nisra = Option(row.getAs[Seq[Row]]("nisra")).getOrElse(Seq())
        val parentUprn = Option(row.getAs[Long]("parentUprn"))
        val classifications = Option(row.getAs[Seq[Row]]("classification")).getOrElse(Seq())
        val crossRefs = Option(row.getAs[Seq[Row]]("crossRefs")).getOrElse(Seq())
        val outputCrossRefs = crossRefs.map(row => HybridAddressEsDocument.rowToCrossRef(row))
        val outputLpis = lpis.map(row => HybridAddressSkinnyNisraEsDocument.rowToLpi(row))
        val outputPaf = paf.map(row => HybridAddressSkinnyNisraEsDocument.rowToPaf(row))
        val outputNisra = nisra.map(row => HybridAddressSkinnyNisraEsDocument.rowToNisra(row))
        val addressType = Option(row.getAs[String]("addressType")).getOrElse("")
        val estabType = Option(row.getAs[String]("estabType")).getOrElse("")
        val nisraAddressType = Try(outputNisra.headOption.get("addressType").toString).toOption
        val testNisra = outputNisra.find(_.getOrElse("classificationCode", "") != "")
        val nisraClassCode: String = if (nisraAddressType.getOrElse("").equals("HH")) "RD" else
                    Try(testNisra.flatMap(_.get("classificationCode").map(_.toString)).getOrElse("")).getOrElse("")
        val classificationCode: Option[String] = {
          if (nisraClassCode.isEmpty)
            classifications.map(row => row.getAs[String]("classificationCode")).headOption
          else
            Some(nisraClassCode)
        }

        val isCouncilTax:Boolean = outputCrossRefs.mkString.contains("7666VC")
        val isNonDomesticRate:Boolean = outputCrossRefs.mkString.contains("7666VN")

        val censusAddressType = if (nisraAddressType.isEmpty || nisraAddressType.get.isEmpty) {
          if (addressType.isEmpty) CensusClassificationHelper.ABPToAddressType(classificationCode.getOrElse(""), isCouncilTax, isNonDomesticRate) else addressType
        } else
          nisraAddressType.getOrElse("NA")

        val nisraEstabType = Try(StringUtil.applyTitleCasing(outputNisra.headOption.get("estabType").toString)).toOption
        val censusEstabType = if (nisraEstabType.isEmpty || nisraEstabType.get.isEmpty)
          if (estabType.isEmpty) CensusClassificationHelper.ABPToEstabType(classificationCode.getOrElse(""), isCouncilTax, isNonDomesticRate) else StringUtil.applyTitleCasing(estabType)
        else
          nisraEstabType.getOrElse("NA")

        val lpiPostCode: Option[String] = outputLpis.headOption.flatMap(_.get("postcodeLocator").map(_.toString))
        val pafPostCode: Option[String] = outputPaf.headOption.flatMap(_.get("postcode").map(_.toString))
        val nisraPostCode: String = Try(outputNisra.headOption.get("postcode").toString).getOrElse("")
        val postCode = if (nisraPostCode != "") nisraPostCode
        else if (pafPostCode.isDefined) pafPostCode.getOrElse("")
        else lpiPostCode.getOrElse("")

        val fromSource = if (outputNisra.headOption.nonEmpty) "NI" else "EW"

        val countryCode = if (outputNisra.headOption.nonEmpty) "N" else outputLpis.headOption.flatMap(_.get("country").map(_.toString)).getOrElse("E")

        val englishNag = outputLpis.find(_.getOrElse("language", "ENG") == "ENG")

        val nisraStreet: Option[String] = Try(outputNisra.headOption.get("thoroughfare").toString).toOption
        val nisraTown: Option[String] = Try(outputNisra.headOption.get("townName").toString).toOption
        val nisraStart: Option[String] = outputNisra.headOption.flatMap(_.get("mixedNisraStart").map(_.toString))

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
        val bestStreet: String = if (!nisraStreet.getOrElse("").isEmpty) nisraStreet.getOrElse("")
        else if (!pafStreet.getOrElse("").isEmpty) pafStreet.getOrElse("")
        else if (!lpiStreetEng.getOrElse("").isEmpty) lpiStreetEng.getOrElse("")
        else if (!lpiStreet.getOrElse("").isEmpty) lpiStreet.getOrElse("")
        else if (!nisraStart.getOrElse("").isEmpty) "(" + nisraStart.getOrElse("") + ")"
        else if (!lpiStartEng.getOrElse("").isEmpty) "(" + lpiStartEng.getOrElse("") + ")"
        else "(" + lpiStart.getOrElse("") + ")"

        val bestTown: String = if (!nisraTown.getOrElse("").isEmpty) nisraTown.getOrElse("")
        else if (!pafDepend.getOrElse("").isEmpty) pafDepend.getOrElse("")
        else if (!lpiLocalityEng.getOrElse("").isEmpty) lpiLocalityEng.getOrElse("")
        else if (!lpiLocality.getOrElse("").isEmpty) lpiLocality.getOrElse("")
        else if (!lpiTownEng.getOrElse("").isEmpty) lpiTownEng.getOrElse("")
        else if (!lpiTown.getOrElse("").isEmpty) lpiTown.getOrElse("")
        else pafTown.getOrElse("")

        val postcodeStreetTown = (postCode + "_" + bestStreet + "_" + bestTown).replace(".","").replace("'","")
        val postTown = pafTown.orNull

        val mixedKeys = List("mixedNag", "mixedWelshNag", "mixedPaf", "mixedWelshPaf", "mixedNisra")
        val mixedPartial = (outputLpis ++ outputPaf ++ outputNisra).flatMap( mixedKeys collect _ )
        val mixedPartialTokens = mixedPartial.flatMap(_.toString.split(",").filter(_.nonEmpty)).distinct.mkString(",")

        HybridAddressSkinnyNisraEsDocument(
          uprn,
          parentUprn.getOrElse(0L),
          outputLpis,
          outputPaf,
          outputNisra,
          classificationCode,
          censusAddressType,
          censusEstabType,
          postCode,
          fromSource,
          countryCode,
          postcodeStreetTown,
          postTown,
          mixedPartialTokens
        )
    }
  }

  /**
    * Constructs a hybrid index from nag and paf dataframes
    */
  def aggregateHybridSkinnyIndex(paf: DataFrame, nag: DataFrame, historical: Boolean = true, nisraAddress1YearAgo: Boolean = false): RDD[HybridAddressSkinnyEsDocument] = {

    // If non-historical there could be zero lpis associated with the PAF record since historical lpis were filtered
    // out at the joinCsvs stage. These need to be removed.
    val pafGrouped = createPafGrouped(paf, nag, historical)

    // DataFrame of paf and lpis by uprn
    val pafNagGrouped = createPafNagGrouped(nag, pafGrouped)

    val pafNagHierGrouped = createPafNagHierGrouped(pafNagGrouped)

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
        val censusAddressType = if (addressType.isEmpty) CensusClassificationHelper.ABPToAddressType(classificationCode.getOrElse(""), isCouncilTax, isNonDomesticRate) else addressType
        val censusEstabType = if (estabType.isEmpty) CensusClassificationHelper.ABPToEstabType(classificationCode.getOrElse(""), isCouncilTax, isNonDomesticRate) else StringUtil.applyTitleCasing(estabType)


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
        val bestStreet: String = if (!pafStreet.getOrElse("").isEmpty) pafStreet.getOrElse("")
        else if (!lpiStreetEng.getOrElse("").isEmpty) lpiStreetEng.getOrElse("")
        else if (!lpiStreet.getOrElse("").isEmpty) lpiStreet.getOrElse("")
        else if (!lpiStartEng.getOrElse("").isEmpty) "(" + lpiStartEng.getOrElse("") + ")"
        else "(" + lpiStart.getOrElse("") + ")"

        val bestTown: String = if (!pafDepend.getOrElse("").isEmpty) pafDepend.getOrElse("")
        else if (!lpiLocalityEng.getOrElse("").isEmpty) lpiLocalityEng.getOrElse("")
        else if (!lpiLocality.getOrElse("").isEmpty) lpiLocality.getOrElse("")
        else if (!lpiTownEng.getOrElse("").isEmpty) lpiTownEng.getOrElse("")
        else if (!lpiTown.getOrElse("").isEmpty) lpiTown.getOrElse("")
        else pafTown.getOrElse("")

        val postcodeStreetTown = (postCode + "_" + bestStreet + "_" + bestTown).replace(".","").replace("'","")
        val postTown = pafTown.orNull

        val mixedKeys = List("mixedNag", "mixedWelshNag", "mixedPaf", "mixedWelshPaf")
        val mixedPartial = (outputLpis ++ outputPaf).flatMap( mixedKeys collect _ )
        val mixedPartialTokens = mixedPartial.flatMap(_.toString.split(",").filter(_.nonEmpty)).distinct.mkString(",")

        HybridAddressSkinnyEsDocument(
          uprn,
          parentUprn.getOrElse(0L),
          outputLpis,
          outputPaf,
          classificationCode,
          censusAddressType,
          censusEstabType,
          postCode,
          fromSource,
          countryCode,
          postcodeStreetTown,
          postTown,
          mixedPartialTokens
        )
    }
  }

  /**
    * Constructs a hybrid index from nag and paf dataframes
    */
  def aggregateHybridNisraIndex(paf: DataFrame, nag: DataFrame, nisra: DataFrame, historical: Boolean = true, nisraAddress1YearAgo: Boolean = false): RDD[HybridAddressNisraEsDocument] = {

    // If non-historical there could be zero lpis associated with the PAF record since historical lpis were filtered
    // out at the joinCsvs stage. These need to be removed.
    val pafGrouped = createPafGrouped(paf, nag, historical)

    // DataFrame of nisra by uprn
    val nisraGrouped = createNisraGrouped(nisra, nisraAddress1YearAgo)

    // DataFrame of paf and lpis by uprn
    val pafNagGrouped = createPafNagGrouped(nag, pafGrouped)
      .join(nisraGrouped, Seq("uprn"), "full_outer")

    val pafNagCrossHierGrouped = createPafNagHierGroupedWithRelatives(pafNagGrouped)

    pafNagCrossHierGrouped.rdd.map {
      row =>
        val uprn = row.getAs[Long]("uprn")
        val paf = Option(row.getAs[Seq[Row]]("paf")).getOrElse(Seq())
        val lpis = Option(row.getAs[Seq[Row]]("lpis")).getOrElse(Seq())
        val crossRefs = Option(row.getAs[Seq[Row]]("crossRefs")).getOrElse(Seq())
        val relatives = Option(row.getAs[Seq[Row]]("relatives")).getOrElse(Seq())
        val nisra = Option(row.getAs[Seq[Row]]("nisra")).getOrElse(Seq())
        val parentUprn = Option(row.getAs[Long]("parentUprn"))
        val classifications = Option(row.getAs[Seq[Row]]("classification")).getOrElse(Seq())
        val addressType = Option(row.getAs[String]("addressType")).getOrElse("")
        val estabType = Option(row.getAs[String]("estabType")).getOrElse("")
        val outputLpis = lpis.map(row => HybridAddressNisraEsDocument.rowToLpi(row))
        val outputPaf = paf.map(row => HybridAddressNisraEsDocument.rowToPaf(row))
        val outputCrossRefs = crossRefs.map(row => HybridAddressNisraEsDocument.rowToCrossRef(row))
        val outputRelatives = relatives.map(row => HybridAddressNisraEsDocument.rowToHierarchy(row))
        val outputNisra = nisra.map(row => HybridAddressNisraEsDocument.rowToNisra(row))
        val nisraAddressType = Try(outputNisra.headOption.get("addressType").toString).toOption

        val testNisra = outputNisra.find(_.getOrElse("classificationCode", "") != "")
        val nisraClassCode: String = if (nisraAddressType.getOrElse("").equals("HH")) "RD" else
          Try(testNisra.flatMap(_.get("classificationCode").map(_.toString)).getOrElse("")).getOrElse("")
        val classificationCode: Option[String] = {
          if (nisraClassCode.isEmpty)
            classifications.map(row => row.getAs[String]("classificationCode")).headOption
          else
            Some(nisraClassCode)
        }

        val isCouncilTax:Boolean = outputCrossRefs.mkString.contains("7666VC")
        val isNonDomesticRate:Boolean = outputCrossRefs.mkString.contains("7666VN")

        val censusAddressType = if (nisraAddressType.isEmpty || nisraAddressType.get.isEmpty)
          if (addressType.isEmpty) CensusClassificationHelper.ABPToAddressType(classificationCode.getOrElse(""), isCouncilTax, isNonDomesticRate) else addressType
        else
          nisraAddressType.getOrElse("NA")

        val nisraEstabType = Try(StringUtil.applyTitleCasing(outputNisra.headOption.get("estabType").toString)).toOption
        val censusEstabType = if (nisraEstabType.isEmpty || nisraEstabType.get.isEmpty)
          if (estabType.isEmpty) CensusClassificationHelper.ABPToEstabType(classificationCode.getOrElse(""), isCouncilTax, isNonDomesticRate) else StringUtil.applyTitleCasing(estabType)
        else
          nisraEstabType.getOrElse("NA")

        val lpiPostCode: Option[String] = outputLpis.headOption.flatMap(_.get("postcodeLocator").map(_.toString))
        val pafPostCode: Option[String] = outputPaf.headOption.flatMap(_.get("postcode").map(_.toString))
        val nisraPostCode: String = Try(outputNisra.headOption.get("postcode").toString).getOrElse("")
        val postCode = if (nisraPostCode != "") nisraPostCode
        else if (pafPostCode.isDefined) pafPostCode.getOrElse("")
        else lpiPostCode.getOrElse("")
        val splitPostCode = postCode.split(" ")
        val (postCodeOut, postCodeIn) =
          if (splitPostCode.size == 2 && splitPostCode(1).length == 3) (splitPostCode(0), splitPostCode(1))
          else ("", "")

        val fromSource = if (outputNisra.headOption.nonEmpty) "NI" else "EW"

        val countryCode = if (outputNisra.headOption.nonEmpty) "N" else outputLpis.headOption.flatMap(_.get("country").map(_.toString)).getOrElse("E")

        val englishNag = outputLpis.find(_.getOrElse("language", "ENG") == "ENG")

        val nisraStreet: Option[String] = Try(outputNisra.headOption.get("thoroughfare").toString).toOption
        val nisraTown: Option[String] = Try(outputNisra.headOption.get("townName").toString).toOption
        val nisraStart: Option[String] = outputNisra.headOption.flatMap(_.get("mixedNisraStart").map(_.toString))

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
        val bestStreet: String = if (!nisraStreet.getOrElse("").isEmpty) nisraStreet.getOrElse("")
        else if (!pafStreet.getOrElse("").isEmpty) pafStreet.getOrElse("")
        else if (!lpiStreetEng.getOrElse("").isEmpty) lpiStreetEng.getOrElse("")
        else if (!lpiStreet.getOrElse("").isEmpty) lpiStreet.getOrElse("")
        else if (!nisraStart.getOrElse("").isEmpty) "(" + nisraStart.getOrElse("") + ")"
        else if (!lpiStartEng.getOrElse("").isEmpty) "(" + lpiStartEng.getOrElse("") + ")"
        else "(" + lpiStart.getOrElse("") + ")"

        val bestTown: String = if (!nisraTown.getOrElse("").isEmpty) nisraTown.getOrElse("")
        else if (!pafDepend.getOrElse("").isEmpty) pafDepend.getOrElse("")
        else if (!lpiLocalityEng.getOrElse("").isEmpty) lpiLocalityEng.getOrElse("")
        else if (!lpiLocality.getOrElse("").isEmpty) lpiLocality.getOrElse("")
        else if (!lpiTownEng.getOrElse("").isEmpty) lpiTownEng.getOrElse("")
        else if (!lpiTown.getOrElse("").isEmpty) lpiTown.getOrElse("")
        else pafTown.getOrElse("")

        val postcodeStreetTown = (postCode + "_" + bestStreet + "_" + bestTown).replace(".","").replace("'","")
        val postTown = pafTown.orNull

        val mixedKeys = List("mixedNag", "mixedWelshNag", "mixedPaf", "mixedWelshPaf", "mixedNisra")
        val mixedPartial = (outputLpis ++ outputPaf ++ outputNisra).flatMap( mixedKeys collect _ )
        val mixedPartialTokens = mixedPartial.flatMap(_.toString.split(",").filter(_.nonEmpty)).distinct.mkString(",")

        HybridAddressNisraEsDocument(
          uprn,
          postCodeIn,
          postCodeOut,
          parentUprn.getOrElse(0L),
          outputRelatives,
          outputLpis,
          outputPaf,
          outputCrossRefs,
          outputNisra,
          classificationCode,
          censusAddressType,
          censusEstabType,
          postCode,
          fromSource,
          countryCode,
          postcodeStreetTown,
          postTown,
          mixedPartialTokens
        )
    }
  }

  /**
    * Constructs a hybrid index from nag and paf dataframes
    */
  def aggregateHybridIndex(paf: DataFrame, nag: DataFrame, historical: Boolean = true): RDD[HybridAddressEsDocument] = {

    // If non-historical there could be zero lpis associated with the PAF record since historical lpis were filtered
    // out at the joinCsvs stage. These need to be removed.
    val pafGrouped = createPafGrouped(paf, nag, historical)

    // DataFrame of paf and lpis by uprn
    val pafNagGrouped = createPafNagGrouped(nag, pafGrouped)

    val pafNagCrossHierGrouped = createPafNagHierGroupedWithRelatives(pafNagGrouped)

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
        val censusAddressType = if (addressType.isEmpty) CensusClassificationHelper.ABPToAddressType(classificationCode.getOrElse(""), isCouncilTax, isNonDomesticRate) else addressType
        val censusEstabType = if (estabType.isEmpty) CensusClassificationHelper.ABPToEstabType(classificationCode.getOrElse(""), isCouncilTax, isNonDomesticRate) else StringUtil.applyTitleCasing(estabType)

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
        val bestStreet: String = if (!pafStreet.getOrElse("").isEmpty) pafStreet.getOrElse("")
        else if (!lpiStreetEng.getOrElse("").isEmpty) lpiStreetEng.getOrElse("")
        else if (!lpiStreet.getOrElse("").isEmpty) lpiStreet.getOrElse("")
        else if (!lpiStartEng.getOrElse("").isEmpty) "(" + lpiStartEng.getOrElse("") + ")"
        else "(" + lpiStart.getOrElse("") + ")"

        val bestTown: String = if (!pafDepend.getOrElse("").isEmpty) pafDepend.getOrElse("")
        else if (!lpiLocalityEng.getOrElse("").isEmpty) lpiLocalityEng.getOrElse("")
        else if (!lpiLocality.getOrElse("").isEmpty) lpiLocality.getOrElse("")
        else if (!lpiTownEng.getOrElse("").isEmpty) lpiTownEng.getOrElse("")
        else if (!lpiTown.getOrElse("").isEmpty) lpiTown.getOrElse("")
        else pafTown.getOrElse("")

        val postcodeStreetTown = (postCode + "_" + bestStreet + "_" + bestTown).replace(".","").replace("'","")
        val postTown = pafTown.orNull

        val mixedKeys = List("mixedNag", "mixedWelshNag", "mixedPaf", "mixedWelshPaf")
        val mixedPartial = (outputLpis ++ outputPaf).flatMap( mixedKeys collect _ )
        val mixedPartialTokens = mixedPartial.flatMap(_.toString.split(",").filter(_.nonEmpty)).distinct.mkString(",")

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
          censusAddressType,
          censusEstabType,
          postCode,
          fromSource,
          countryCode,
          postcodeStreetTown,
          postTown,
          mixedPartialTokens
        )
    }
  }

  // function moved into calling code due to strange Spark glitch
  private def nisraCodeToABP(ncode: String): String = ncode match {

    case "DO_DETACHED" => "RD02"
    case "DO_SEMI" => "RD03"
    case "ND_RETAIL" => "CR"
    case "NON_POSTAL" => "O"
    case "DO_TERRACE" => "RD04"
    case "ND_ENTERTAINMENT" => "CL"
    case "ND_HOSPITALITY" => "CH"
    case "ND_SPORTING" => "CL06"
    case "DO_APART" => "RD06"
    case "ND_INDUSTRY" => "CI"
    case "ND_EDUCATION" => "CE"
    case "ND_RELIGIOUS" => "ZW"
    case "ND_COMM_OTHER" => "C"
    case "ND_OTHER" =>   "C"
    case "ND_AGRICULTURE" => "CA"
    case "DO_OTHER" => "RD"
    case "ND_OFFICE" => "CO"
    case "ND_HEALTH" => "CM"
    case "ND_LEGAL" => "CC02"
    case "ND_CULTURE" => "CL04"
    case "ND_ENTS_OTHER" => "CL"
    case "ND_CULTURE_OTHER" => "CL04"
    case "ND_INDUST_OTHER" => "CI"
    case _ => "O"
  }

}