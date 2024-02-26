package uk.gov.ons.addressindex.models

import org.apache.spark.sql.Row
import uk.gov.ons.addressindex.models.HybridAddressEsDocument.{addLeadingZeros, concatNag, concatPaf, generateFormattedNagAddress, generateFormattedPafAddress, generateWelshFormattedPafAddress, normalize, normalizeTowns}

trait HybridAddress {

  def rowToLpi(row: Row): Map[String, Any] = Map(
    "uprn" -> row.getLong(0),
    "postcodeLocator" -> row.getString(1).trim,
    "addressBasePostal" -> row.getString(2),
    "location" -> row.get(3),
    "easting" -> (if (row.isNullAt(4)) 0 else row.getFloat(4)),
    "northing" -> (if (row.isNullAt(5)) 0 else row.getFloat(5)),
    "parentUprn" -> (if (row.isNullAt(6)) null else row.getLong(6)),
    "multiOccCount" -> row.getShort(7),
    "blpuLogicalStatus" -> row.getByte(8),
    "localCustodianCode" -> row.getShort(9),
    "rpc" -> row.getByte(10),
    "organisation" -> normalize(row.getString(11)),
    "legalName" -> row.getString(12),
    "usrn" -> (if (row.isNullAt(13)) null else row.getInt(13)),
    "lpiKey" -> row.getString(14),
    "paoText" -> normalize(row.getString(15)),
    "paoStartNumber" -> (if (row.isNullAt(16)) null else row.getShort(16)),
    "paoStartSuffix" -> row.getString(17),
    "paoEndNumber" -> (if (row.isNullAt(18)) null else row.getShort(18)),
    "paoEndSuffix" -> row.getString(19),
    "saoText" -> normalize(row.getString(20)),
    "saoStartNumber" -> (if (row.isNullAt(21)) null else row.getShort(21)),
    "saoStartSuffix" -> row.getString(22),
    "saoEndNumber" -> (if (row.isNullAt(23)) null else row.getShort(23)),
    "saoEndSuffix" -> row.getString(24),
    "level" -> row.getString(25),
    "officialFlag" -> row.getString(26),
    "lpiLogicalStatus" -> row.getByte(27),
    "usrnMatchIndicator" -> (if (row.isNullAt(28)) null else row.getByte(28)),
    "language" -> row.getString(29),
    "streetDescriptor" -> normalize(row.getString(30)),
    "townName" -> normalizeTowns(row.getString(31)),
    "locality" -> normalize(row.getString(32)),
    "streetClassification" -> (if (row.isNullAt(33)) null else row.getByte(33)),
    "lpiStartDate" -> row.getDate(34),
    "lpiLastUpdateDate" -> row.getDate(35),
    "lpiEndDate" -> row.getDate(36),
    "country" -> row.getString(37),
    "nagAll" -> concatNag(
      if (row.isNullAt(21)) "" else row.getShort(21).toString,
      if (row.isNullAt(23)) "" else row.getShort(23).toString,
      row.getString(24), row.getString(22), row.getString(20), row.getString(11),
      if (row.isNullAt(16)) "" else row.getShort(16).toString,
      row.getString(17),
      if (row.isNullAt(18)) "" else row.getShort(18).toString,
      row.getString(19), row.getString(15), row.getString(30),
      row.getString(31), row.getString(32), row.getString(1).trim
    ),
    "mixedNag" -> (if (row.getString(29) != "ENG") "" else generateNagAddress(row)),
    "mixedWelshNag" -> (if (row.getString(29) == "ENG") "" else generateNagAddress(row)),
    "mixedNagStart" -> (if (row.getString(29) != "ENG") "" else generateNagAddress(row))
      .replaceAll(",","").replaceAll("'","").take(12),
    "mixedWelshNagStart" -> (if (row.getString(29) == "ENG") "" else generateNagAddress(row))
      .replaceAll(",","").replaceAll("'","").take(12),
    "secondarySort" -> addLeadingZeros(row.getString(15) + " " + (if (row.isNullAt(21)) "" else row.getShort(21).toString) + row.getString(22) + " " + row.getString(11) + " " + row.getString(20)).replaceAll(" +", " ")
  )

  private def generateNagAddress(row: Row): String =
    generateFormattedNagAddress(
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
      row.getString(1).trim + " " + row.getString(1).replace(" ","")
    )

  def rowToPaf(row: Row): Map[String, Any] = Map(
    "recordIdentifier" -> row.getByte(0),
    "changeType" -> Option(row.getString(1)).getOrElse(""),
    "proOrder" -> row.getLong(2),
    "uprn" -> row.getLong(3),
    "udprn" -> row.getInt(4),
    "organisationName" -> normalize(Option(row.getString(5)).getOrElse("")),
    "departmentName" -> normalize(Option(row.getString(6)).getOrElse("")),
    "subBuildingName" -> normalize(Option(row.getString(7)).getOrElse("")),
    "buildingName" -> normalize(Option(row.getString(8)).getOrElse("")),
    "buildingNumber" -> (if (row.isNullAt(9)) null else row.getShort(9)),
    "dependentThoroughfare" -> normalize(Option(row.getString(10)).getOrElse("")),
    "thoroughfare" -> normalize(Option(row.getString(11)).getOrElse("")),
    "doubleDependentLocality" -> normalize(Option(row.getString(12)).getOrElse("")),
    "dependentLocality" -> normalize(Option(row.getString(13)).getOrElse("")),
    "postTown" -> normalizeTowns(Option(row.getString(14)).getOrElse("")),
    "postcode" -> Option(row.getString(15)).getOrElse(""),
    "postcodeType" -> Option(row.getString(16)).getOrElse(""),
    "deliveryPointSuffix" -> Option(row.getString(17)).getOrElse(""),
    "welshDependentThoroughfare" -> normalize(Option(row.getString(18)).getOrElse("")),
    "welshThoroughfare" -> normalize(Option(row.getString(19)).getOrElse("")),
    "welshDoubleDependentLocality" -> normalize(Option(row.getString(20)).getOrElse("")),
    "welshDependentLocality" -> normalize(Option(row.getString(21)).getOrElse("")),
    "welshPostTown" -> normalizeTowns(Option(row.getString(22)).getOrElse("")),
    "poBoxNumber" -> Option(row.getString(23)).getOrElse(""),
    "processDate" -> row.getDate(24),
    "startDate" -> row.getDate(25),
    "endDate" -> row.getDate(26),
    "lastUpdateDate" -> row.getDate(27),
    "entryDate" -> row.getDate(28),
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
      Option(row.getString(15)).getOrElse("") + " " + Option(row.getString(15)).getOrElse("").replace(" ","")
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
      Option(row.getString(15)).getOrElse("") + " " + Option(row.getString(15)).getOrElse("").replace(" ","")
    ),
    "mixedPafStart" -> generateFormattedPafAddress(
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
      Option(row.getString(15)).getOrElse("") + " " + Option(row.getString(15)).getOrElse("").replace(" ","")
    ).replaceAll(",","").replaceAll("'","").take(12),
    "mixedWelshPafStart" -> generateWelshFormattedPafAddress(
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
      Option(row.getString(15)).getOrElse("") + " " + Option(row.getString(15)).getOrElse("").replace(" ","")
    ).replaceAll(",","").replaceAll("'","").take(12)
  )

  def rowToHierarchy(row: Row): Map[String, Any] = Map(
    "level" -> row.getAs[String]("level"),
    "siblings" -> row.getAs[Array[Long]]("siblings"),
    "parents" -> row.getAs[Array[Long]]("parents")
  )

  def rowToCrossRef(row: Row): Map[String, String] = Map(
    "crossReference" -> row.getAs[String]("crossReference"),
    "source" -> row.getAs[String]("source")
  )

}
