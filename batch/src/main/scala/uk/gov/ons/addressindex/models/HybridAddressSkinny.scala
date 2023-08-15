package uk.gov.ons.addressindex.models

import org.apache.spark.sql.Row
import uk.gov.ons.addressindex.models.HybridAddressSkinnyEsDocument.{addLeadingZeros, concatNag, generateFormattedNagAddress, generateFormattedPafAddress, generateWelshFormattedPafAddress, normalize, normalizeTowns}

trait HybridAddressSkinny {

  def rowToLpi(row: Row): Map[String, Any] = Map(
    "uprn" -> row.getLong(0),
    "postcodeLocator" -> row.getString(1).trim,
    "addressBasePostal" -> row.getString(2),
    "location" -> row.get(3),
    "easting" -> (if (row.isNullAt(4)) 0 else row.getFloat(4)),
    "northing" -> (if (row.isNullAt(5)) 0 else row.getFloat(5)),
    "parentUprn" -> (if (row.isNullAt(6)) null else row.getLong(6)),
    "paoStartNumber" -> (if (row.isNullAt(16)) null else row.getShort(16)),
    "paoStartSuffix" -> row.getString(17),
    "saoStartNumber" -> (if (row.isNullAt(21)) null else row.getShort(21)),
    "lpiLogicalStatus" -> row.getByte(27),
    "language" -> row.getString(29),
    "streetDescriptor" -> normalize(row.getString(30)),
    "locality" -> normalizeTowns(row.getString(32)),
    "townName" -> normalizeTowns(row.getString(31)),
    "country" -> row.getString(37),
    "nagAll" -> concatNag(
      if (row.isNullAt(21)) "" else row.getShort(21).toString,
      if (row.isNullAt(23)) "" else row.getShort(23).toString,
      row.getString(24), row.getString(22), row.getString(20), row.getString(11),
      if (row.isNullAt(16)) "" else row.getShort(16).toString,
      row.getString(17),
      if (row.isNullAt(18)) "" else row.getShort(18).toString,
      row.getString(19), row.getString(15), row.getString(30), row.getString(31),
      row.getString(32), row.getString(1).trim
    ),
    "mixedNag" -> (if (row.getString(29) != "ENG") "" else generateNagAddress(row)),
    "mixedWelshNag" -> (if (row.getString(29) == "ENG") "" else generateNagAddress(row)),
    "mixedNagStart" -> (if (row.getString(29) != "ENG") "" else generateNagAddress(row))
      .replaceAll(",","").replaceAll("'","").take(12),
    "mixedWelshNagStart" -> (if (row.getString(29) == "ENG") "" else generateNagAddress(row))
      .replaceAll(",","").replaceAll("'","").take(12),
    "secondarySort" -> addLeadingZeros(row.getString(15) + " " + (if (row.isNullAt(21)) "" else row.getShort(21).toString) + row.getString(22) + " " + row.getString(11) + " " + row.getString(20)).replaceAll(" +", " ")
  )

  def rowToPaf(row: Row): Map[String, Any] = Map(
    "uprn" -> row.getLong(3),
    "thoroughfare" -> normalize(Option(row.getString(11)).getOrElse("")),
    "postTown" -> normalizeTowns(Option(row.getString(14)).getOrElse("")),
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

}
