package uk.gov.ons.addressindex.models

import org.apache.spark.sql.Row

case class HybridAddressSkinnyNisraEsDocument(uprn: Long,
                                              parentUprn: Long,
                                              lpi: Seq[Map[String, Any]],
                                              paf: Seq[Map[String, Any]],
                                              nisra: Seq[Map[String, Any]],
                                              classificationCode: Option[String],
                                              censusAddressType: String,
                                              censusEstabType: String,
                                              postcode: String,
                                              fromSource: String,
                                              countryCode: String,
                                              postcodeStreetTown: String,
                                              postTown: String,
                                              mixedPartial: String,
                                              onsAddressId: Option[Long],
                                              addressEntryId: Option[Long],
                                              addressEntryIdAlphanumericBackup: Option[String])

object HybridAddressSkinnyNisraEsDocument extends EsDocument with HybridAddressSkinny with NisraAddress {

  def rowToNisra(row: Row): Map[String, Any] = {

    val nisraFormatted: Array[String] = createNisraFormatted(row)

    Map(
      "uprn" -> row.getLong(0),
      "buildingNumber" -> toShort(row.getString(3)).orNull,
      "easting" -> (if (row.isNullAt(21)) 0F else row.getFloat(21)),
      "northing" -> (if (row.isNullAt(22)) 0F else row.getFloat(22)),
      "location" -> row.get(23),
      "paoStartNumber" -> toShort(row.getString(4)).orNull,
      "saoStartNumber" -> toShort(row.getString(9)).orNull,
      "classificationCode" -> row.getString(24),
      "thoroughfare" -> normalize(Option(row.getString(15)).getOrElse("")),
      "townName" -> normalize(Option(row.getString(19)).getOrElse("")),
      "mixedNisra" -> nisraFormatted(0),
      "mixedNisraStart" -> nisraFormatted(0).replaceAll(",","").replaceAll("'","").take(12),
      "nisraAll" -> nisraFormatted(2),
      "postcode" -> row.getString(20),
      "secondarySort" -> addLeadingZeros(Option(row.getString(8)).getOrElse("") + " " + Option(row.getString(9)).getOrElse("") + Option(row.getString(11)).getOrElse("") + " " + Option(row.getString(13)).getOrElse("") + " " + Option(row.getString(14)).getOrElse("")).replaceAll(" +", " "),
      "localCustodianCode" -> Option(row.getString(26)).getOrElse(""),
      "addressType" -> Option(row.getString(29)).getOrElse("").trim,
      "estabType" -> normalize(Option(row.getString(30)).getOrElse("")).trim,
      "addressLine1" -> normalize(Option(row.getString(41)).getOrElse("")),
      "addressLine2" -> normalize(Option(row.getString(42)).getOrElse("")),
      "addressLine3" -> normalize(Option(row.getString(43)).getOrElse("")),
      "postTown" -> normalize(Option(row.getString(45)).getOrElse(""))
    )
  }

}