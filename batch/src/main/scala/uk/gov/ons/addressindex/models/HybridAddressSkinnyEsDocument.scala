package uk.gov.ons.addressindex.models

import org.apache.spark.sql.Row

case class HybridAddressSkinnyEsDocument(
                                    uprn: Long,
                                    parentUprn: Long,
                                    lpi: Seq[Map[String, Any]],
                                    paf: Seq[Map[String, Any]],
                                    nisra: Seq[Map[String, Any]],
                                    classificationCode: Option[String]
                                  )

object HybridAddressSkinnyEsDocument extends EsDocument {

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
    "streetDescriptor" -> splitAndCapitaliseTowns(row.getString(30)),
    "lpiStartDate" -> row.getDate(34),
    "lpiEndDate" -> row.getDate(36),
    "nagAll" ->  concatNag(
      if (row.isNullAt(21)) "" else row.getShort(21).toString,
      if (row.isNullAt(23)) "" else row.getShort(23).toString,
      row.getString(24), row.getString(22), row.getString(20), row.getString(11),
      if (row.isNullAt(16)) "" else row.getShort(16).toString,
      row.getString(17),
      if (row.isNullAt(18)) "" else row.getShort(18).toString,
      row.getString(19), row.getString(15), row.getString(30),
      row.getString(31), row.getString(32), row.getString(1)
    ),
    "mixedNag" -> generateFormattedNagAddress(
      if (row.isNullAt(21)) "" else row.getShort(21).toString,
      row.getString(22),
      if (row.isNullAt(23)) "" else row.getShort(23).toString,
      row.getString(24),
      splitAndCapitaliseTowns(row.getString(20)),
      splitAndCapitaliseTowns(row.getString(11)),
      if (row.isNullAt(16)) "" else row.getShort(16).toString,
      row.getString(17),
      if (row.isNullAt(18)) "" else row.getShort(18).toString,
      row.getString(19),
      splitAndCapitaliseTowns(row.getString(15)),
      splitAndCapitaliseTowns(row.getString(30)),
      splitAndCapitaliseTowns(row.getString(32)),
      splitAndCapitaliseTowns(row.getString(31)),
      row.getString(1)
    )
  )

  def rowToPaf(row: Row): Map[String, Any] = Map(
    "uprn" -> row.getLong(3),
    "startDate" -> row.getDate(25),
    "endDate" -> row.getDate(26),
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
      splitAndCapitaliseTowns(Option(row.getString(11)).getOrElse("")),
      splitAndCapitaliseTowns(Option(row.getString(6)).getOrElse("")),
      splitAndCapitaliseTowns(Option(row.getString(5)).getOrElse("")),
      splitAndCapitaliseTowns(Option(row.getString(7)).getOrElse("")),
      splitAndCapitaliseTowns(Option(row.getString(8)).getOrElse("")),
      splitAndCapitaliseTowns(Option(row.getString(12)).getOrElse("")),
      splitAndCapitaliseTowns(Option(row.getString(13)).getOrElse("")),
      splitAndCapitaliseTowns(Option(row.getString(14)).getOrElse("")),
      Option(row.getString(15)).getOrElse("")
    ),
    "mixedWelshPaf" -> generateWelshFormattedPafAddress(
      Option(row.getString(23)).getOrElse(""),
      if (row.isNullAt(9)) "" else row.getShort(9).toString,
      splitAndCapitaliseTowns(Option(row.getString(18)).getOrElse(Option(row.getString(10)).getOrElse(""))),
      splitAndCapitaliseTowns(Option(row.getString(19)).getOrElse(Option(row.getString(11)).getOrElse(""))),
      splitAndCapitaliseTowns(Option(row.getString(6)).getOrElse("")),
      splitAndCapitaliseTowns(Option(row.getString(5)).getOrElse("")),
      splitAndCapitaliseTowns(Option(row.getString(7)).getOrElse("")),
      splitAndCapitaliseTowns(Option(row.getString(8)).getOrElse("")),
      splitAndCapitaliseTowns(Option(row.getString(20)).getOrElse(Option(row.getString(12)).getOrElse(""))),
      splitAndCapitaliseTowns(Option(row.getString(21)).getOrElse(Option(row.getString(13)).getOrElse(""))),
      splitAndCapitaliseTowns(Option(row.getString(22)).getOrElse(Option(row.getString(14)).getOrElse(""))),
      Option(row.getString(15)).getOrElse("")
    )
  )

  def rowToNisra(row: Row): Map[String, Any] = {

    val nisraFormatted: Array[String] = generateFormattedNisraAddresses(row.getString(1), row.getString(2),row.getString(3), row.getString(4),
      row.getString(5), row.getString(6), row.getString(7), row.getString(8), row.getString(9), row.getString(10), row.getString(11))

    Map(
      "uprn" -> row.getLong(0),
      "buildingNumber" -> row.getString(4),
      "easting" -> row.getFloat(12),
      "northing" -> row.getFloat(13),
      "location" -> row.get(14),
      "creationDate" -> row.getDate(15),
      "commencementDate" -> row.getDate(16),
      "archivedDate" -> row.getDate(17),
      "mixedNisra" -> nisraFormatted(0),
      "mixedAltNisra" -> nisraFormatted(1),
      "nisraAll" -> nisraFormatted(2)
    )
  }

  def generateFormattedNisraAddresses(organisationName: String, subBuildingName: String, buildingName: String, buildingNumber: String, thoroughfare: String,
                                      altThoroughfare: String, dependentThoroughfare: String, locality: String, townland: String, townName: String,
                                      postcode: String) : Array[String] = {

    val trimmedSubBuildingName = splitAndCapitalise(subBuildingName)
    val trimmedBuildingName = splitAndCapitalise(buildingName)
    val trimmedThoroughfare = splitAndCapitalise(thoroughfare)
    val trimmedAltThoroughfare = splitAndCapitalise(altThoroughfare)
    val trimmedDependentThoroughfare = splitAndCapitalise(dependentThoroughfare)

    val buildingNumberWithStreetDescription = s"${buildingNumber.toUpperCase} $trimmedDependentThoroughfare"

    Array(
      Seq(splitAndCapitalise(organisationName), trimmedSubBuildingName, trimmedBuildingName, buildingNumberWithStreetDescription,
        trimmedThoroughfare, splitAndCapitaliseTowns(locality), splitAndCapitaliseTowns(townland), splitAndCapitaliseTowns(townName),
        postcode.toUpperCase).map(_.trim).filter(_.nonEmpty).mkString(", "),
      if (!altThoroughfare.isEmpty)
      Seq(splitAndCapitalise(organisationName), trimmedSubBuildingName, trimmedBuildingName, buildingNumberWithStreetDescription,
        trimmedAltThoroughfare, splitAndCapitaliseTowns(locality), splitAndCapitaliseTowns(townland), splitAndCapitaliseTowns(townName),
        postcode.toUpperCase).map(_.trim).filter(_.nonEmpty).mkString(", ") else "",
      Seq(organisationName, subBuildingName, buildingName, buildingNumber, dependentThoroughfare, thoroughfare,
        altThoroughfare, locality, townland, townName, postcode).map(_.trim).filter(_.nonEmpty).mkString(" ")
    )
  }
}
