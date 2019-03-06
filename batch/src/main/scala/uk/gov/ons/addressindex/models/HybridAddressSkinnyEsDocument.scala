package uk.gov.ons.addressindex.models

import org.apache.spark.sql.Row

case class HybridAddressSkinnyEsDocument(
                                    uprn: Long,
                                    parentUprn: Long,
                                    lpi: Seq[Map[String, Any]],
                                    paf: Seq[Map[String, Any]],
                                    classificationCode: Option[String],
                                    postcode: String
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
    "streetDescriptor" -> splitAndCapitalise(row.getString(30)),
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
      splitAndCapitalise(row.getString(20)),
      splitAndCapitalise(row.getString(11)),
      if (row.isNullAt(16)) "" else row.getShort(16).toString,
      row.getString(17),
      if (row.isNullAt(18)) "" else row.getShort(18).toString,
      row.getString(19),
      splitAndCapitalise(row.getString(15)),
      splitAndCapitalise(row.getString(30)),
      splitAndCapitalise(row.getString(32)),
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
      splitAndCapitalise(Option(row.getString(11)).getOrElse("")),
      splitAndCapitalise(Option(row.getString(6)).getOrElse("")),
      splitAndCapitalise(Option(row.getString(5)).getOrElse("")),
      splitAndCapitalise(Option(row.getString(7)).getOrElse("")),
      splitAndCapitalise(Option(row.getString(8)).getOrElse("")),
      splitAndCapitalise(Option(row.getString(12)).getOrElse("")),
      splitAndCapitalise(Option(row.getString(13)).getOrElse("")),
      splitAndCapitaliseTowns(Option(row.getString(14)).getOrElse("")),
      Option(row.getString(15)).getOrElse("")
    ),
    "mixedWelshPaf" -> generateWelshFormattedPafAddress(
      Option(row.getString(23)).getOrElse(""),
      if (row.isNullAt(9)) "" else row.getShort(9).toString,
      splitAndCapitalise(Option(row.getString(18)).getOrElse(Option(row.getString(10)).getOrElse(""))),
      splitAndCapitalise(Option(row.getString(19)).getOrElse(Option(row.getString(11)).getOrElse(""))),
      splitAndCapitalise(Option(row.getString(6)).getOrElse("")),
      splitAndCapitalise(Option(row.getString(5)).getOrElse("")),
      splitAndCapitalise(Option(row.getString(7)).getOrElse("")),
      splitAndCapitalise(Option(row.getString(8)).getOrElse("")),
      splitAndCapitalise(Option(row.getString(20)).getOrElse(Option(row.getString(12)).getOrElse(""))),
      splitAndCapitalise(Option(row.getString(21)).getOrElse(Option(row.getString(13)).getOrElse(""))),
      splitAndCapitaliseTowns(Option(row.getString(22)).getOrElse(Option(row.getString(14)).getOrElse(""))),
      Option(row.getString(15)).getOrElse("")
    )
  )
}
