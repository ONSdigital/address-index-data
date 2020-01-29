package uk.gov.ons.addressindex.models

import org.apache.spark.sql.Row

case class HybridAddressSkinnyNisraEsDocument(uprn: Long,
                                              parentUprn: Long,
                                              lpi: Seq[Map[String, Any]],
                                              paf: Seq[Map[String, Any]],
                                              nisra: Seq[Map[String, Any]],
                                              classificationCode: Option[String],
                                              postcode: String,
                                              fromSource: String,
                                              countryCode: String)

object HybridAddressSkinnyNisraEsDocument extends EsDocument {

  def rowToLpi(row: Row): Map[String, Any] = Map(
    "uprn" -> row.getLong(0),
    "postcodeLocator" -> row.getString(1),
    "addressBasePostal" -> row.getString(2),
    "location" -> row.get(3),
    "easting" -> row.getFloat(4),
    "northing" -> row.getFloat(5),
    "parentUprn" -> (if (row.isNullAt(6)) null else row.getLong(6)),
    "paoStartNumber" -> (if (row.isNullAt(16)) null else row.getShort(16)),
    "paoStartSuffix" -> row.getString(17),
    "saoStartNumber" -> (if (row.isNullAt(21)) null else row.getShort(21)),
    "lpiLogicalStatus" -> row.getByte(27),
    "language" -> row.getString(29),
    "streetDescriptor" -> normalize(row.getString(30)),
 //   "lpiStartDate" -> row.getDate(34),
  //  "lpiEndDate" -> row.getDate(36),
    "country" -> row.getString(37),
    "nagAll" -> concatNag(
      if (row.isNullAt(21)) "" else row.getShort(21).toString,
      if (row.isNullAt(23)) "" else row.getShort(23).toString,
      row.getString(24),
      row.getString(22),
      row.getString(20),
      row.getString(11),
      if (row.isNullAt(16)) "" else row.getShort(16).toString,
      row.getString(17),
      if (row.isNullAt(18)) "" else row.getShort(18).toString,
      row.getString(19),
      row.getString(15),
      row.getString(30),
      row.getString(31),
      row.getString(32),
      row.getString(1)
    ),
    "mixedNag" -> generateFormattedNagAddress(
      if (row.isNullAt(21)) "" else row.getShort(21).toString,
      row.getString(22),
      if (row.isNullAt(23)) "" else row.getShort(23).toString,
      row.getString(24),
      normalize(row.getString(20)),
      normalize(row.getString(11)),
      if (row.isNullAt(16)) "" else row.getShort(16).toString,
      row.getString(17),
      if (row.isNullAt(18)) "" else row.getShort(18).toString,
      row.getString(19),
      normalize(row.getString(15)),
      normalize(row.getString(30)),
      normalize(row.getString(32)),
      normalizeTowns(row.getString(31)),
      row.getString(1)
    ),
    "secondarySort" -> addLeadingZeros(row.getString(15) + " " + (if (row.isNullAt(21)) "" else row.getShort(21).toString) + row.getString(22) + " " + row.getString(11) + " " + row.getString(20)).replaceAll(" +", " ")
  )

  def rowToPaf(row: Row): Map[String, Any] = Map(
    "uprn" -> row.getLong(3),
 //   "startDate" -> row.getDate(25),
  //  "endDate" -> row.getDate(26),
    "pafAll" -> concatPaf(Option(row.getString(23)).getOrElse(""),
      if (row.isNullAt(9)) "" else row.getShort(9).toString,
      Option(row.getString(10)).getOrElse(""),
      Option(row.getString(18)).getOrElse(""),
      Option(row.getString(11)).getOrElse(""),
      Option(row.getString(19)).getOrElse(""),
      Option(row.getString(6)).getOrElse(""),
      Option(row.getString(5)).getOrElse(""),
      Option(row.getString(7)).getOrElse(""),
      Option(row.getString(8)).getOrElse(""),
      Option(row.getString(12)).getOrElse(""),
      Option(row.getString(20)).getOrElse(""),
      Option(row.getString(13)).getOrElse(""),
      Option(row.getString(21)).getOrElse(""),
      Option(row.getString(14)).getOrElse(""),
      Option(row.getString(22)).getOrElse(""),
      Option(row.getString(15)).getOrElse("")),
    "mixedPaf" -> generateFormattedPafAddress(
      Option(row.getString(23)).getOrElse(""),
      if (row.isNullAt(9)) "" else row.getShort(9).toString,
      Option(row.getString(10)).getOrElse(""),
      normalize(Option(row.getString(11)).getOrElse("")),
      normalize(Option(row.getString(6)).getOrElse("")),
      normalize(Option(row.getString(5)).getOrElse("")),
      normalize(Option(row.getString(7)).getOrElse("")),
      normalize(Option(row.getString(8)).getOrElse("")),
      normalize(Option(row.getString(12)).getOrElse("")),
      normalize(Option(row.getString(13)).getOrElse("")),
      normalizeTowns(Option(row.getString(14)).getOrElse("")),
      Option(row.getString(15)).getOrElse("")
    ),
    "mixedWelshPaf" -> generateWelshFormattedPafAddress(
      Option(row.getString(23)).getOrElse(""),
      if (row.isNullAt(9)) "" else row.getShort(9).toString,
      normalize(Option(row.getString(18)).getOrElse(Option(row.getString(10)).getOrElse(""))),
      normalize(Option(row.getString(19)).getOrElse(Option(row.getString(11)).getOrElse(""))),
      normalize(Option(row.getString(6)).getOrElse("")),
      normalize(Option(row.getString(5)).getOrElse("")),
      normalize(Option(row.getString(7)).getOrElse("")),
      normalize(Option(row.getString(8)).getOrElse("")),
      normalize(Option(row.getString(20)).getOrElse(Option(row.getString(12)).getOrElse(""))),
      normalize(Option(row.getString(21)).getOrElse(Option(row.getString(13)).getOrElse(""))),
      normalizeTowns(Option(row.getString(22)).getOrElse(Option(row.getString(14)).getOrElse(""))),
      Option(row.getString(15)).getOrElse("")
    )
  )

  def rowToNisra(row: Row): Map[String, Any] = {
    val nisraFormatted: Array[String] = generateFormattedNisraAddresses(
      Option(row.getString(15)).getOrElse(""),
      Option(row.getString(1)).getOrElse(""),
      Option(row.getString(2)).getOrElse(""),
      Option(row.getString(3)).getOrElse(""),
      Option(row.getString(16)).getOrElse(""),
      Option(row.getString(17)).getOrElse(""),
      Option(row.getString(18)).getOrElse(""),
      Option(row.getString(19)).getOrElse(""),
      // townland omitted for now
      "",
      Option(row.getString(21)).getOrElse(""),
      Option(row.getString(22)).getOrElse(""))

    Map(
      "uprn" -> row.getLong(0),
      "buildingNumber" -> toShort(row.getString(3)).orNull,
      "easting" -> row.getFloat(23),
      "northing" -> row.getFloat(24),
      "location" -> row.get(25),
      "addressStatus" -> row.getString(30),
      "paoStartNumber" -> toShort(row.getString(4)).orNull,
      "saoStartNumber" -> toShort(row.getString(9)).orNull,
      "classificationCode" -> row.getString(31),
      "mixedNisra" -> nisraFormatted(0),
      "mixedAltNisra" -> nisraFormatted(1),
      "nisraAll" -> nisraFormatted(2),
      "postcode" -> row.getString(22),
      "secondarySort" -> addLeadingZeros(Option(row.getString(8)).getOrElse("") + " " + Option(row.getString(9)).getOrElse("") + Option(row.getString(11)).getOrElse("") + " " + Option(row.getString(13)).getOrElse("") + " " + Option(row.getString(15)).getOrElse("")).replaceAll(" +", " "),
      "localCouncil" -> row.getString(32)
    )
  }

  def generateFormattedNisraAddresses(organisationName: String, subBuildingName: String, buildingName: String, buildingNumber: String, thoroughfare: String,
                                      altThoroughfare: String, dependentThoroughfare: String, locality: String, townland: String, townName: String,
                                      postcode: String) : Array[String] = {

    val trimmedSubBuildingName = normalize(subBuildingName)
    val trimmedBuildingName = normalize(buildingName)
    val trimmedThoroughfare = normalize(thoroughfare)
    val trimmedAltThoroughfare = normalize(altThoroughfare)
    val trimmedDependentThoroughfare = normalize(dependentThoroughfare)

    val buildingNumberWithStreetDescription = s"${buildingNumber.toUpperCase} $trimmedThoroughfare"
    val buildingNameWithStreetDescription = s"${trimmedBuildingName.toUpperCase} $trimmedThoroughfare"
    val commalessNumberAndStreetPart = if (startsWithNumber.findFirstIn(trimmedBuildingName).isDefined) buildingNameWithStreetDescription else buildingNumberWithStreetDescription

    Array(
      Seq(normalize(organisationName),
        trimmedSubBuildingName,
        if (startsWithNumber.findFirstIn(trimmedBuildingName).isDefined) "" else trimmedBuildingName,
        commalessNumberAndStreetPart,
        trimmedDependentThoroughfare,
        normalizeTowns(locality),
        normalizeTowns(townland),
        normalizeTowns(townName),
        postcode.toUpperCase).map(_.trim).filter(_.nonEmpty).mkString(", "),
      if (!altThoroughfare.isEmpty)
        Seq(normalize(organisationName),
          trimmedSubBuildingName,
          trimmedBuildingName,
          buildingNumber,
          trimmedAltThoroughfare,
          trimmedDependentThoroughfare,
          normalizeTowns(locality),
          normalizeTowns(townland),
          normalizeTowns(townName),
          postcode.toUpperCase).map(_.trim).filter(_.nonEmpty).mkString(", ") else "",
      Seq(organisationName,
        subBuildingName,
        buildingName,
        buildingNumber,
        thoroughfare,
        dependentThoroughfare,
        altThoroughfare,
        locality,
        townland,
        townName,
        postcode).map(_.trim).filter(_.nonEmpty).mkString(" ")
    )
  }
}
