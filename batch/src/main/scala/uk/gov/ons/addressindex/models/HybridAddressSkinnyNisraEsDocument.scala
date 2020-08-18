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
                                              postTown: String)

object HybridAddressSkinnyNisraEsDocument extends EsDocument with HybridAddressSkinny with NisraAddress {

  def rowToNisra(row: Row): Map[String, Any] = {

    val nisraFormatted: Array[String] = createNisraFormatted(row)

    Map(
      "uprn" -> row.getLong(0),
      "buildingNumber" -> toShort(row.getString(3)).orNull,
      "easting" -> row.getFloat(23),
      "northing" -> row.getFloat(24),
      "location" -> row.get(25),
      "addressStatus" -> row.getString(30),
      "paoStartNumber" -> toShort(row.getString(4)).orNull,
      "saoStartNumber" -> toShort(row.getString(9)).orNull,
      "classification" -> row.getString(54),
      "classificationCode" -> row.getString(31),
      "thoroughfare" -> normalize(Option(row.getString(16)).getOrElse("")),
      "townName" -> normalize(Option(row.getString(21)).getOrElse("")),
      "mixedNisra" -> nisraFormatted(0),
      "mixedNisraStart" -> nisraFormatted(0).replaceAll(",","").replaceAll("'","").take(12),
      "mixedAltNisra" -> nisraFormatted(1),
      "nisraAll" -> nisraFormatted(2),
      "postcode" -> row.getString(22),
      "secondarySort" -> addLeadingZeros(Option(row.getString(8)).getOrElse("") + " " + Option(row.getString(9)).getOrElse("") + Option(row.getString(11)).getOrElse("") + " " + Option(row.getString(13)).getOrElse("") + " " + Option(row.getString(15)).getOrElse("")).replaceAll(" +", " "),
      "localCustodianCode" -> Option(row.getString(34)).getOrElse("")
    )
  }

}