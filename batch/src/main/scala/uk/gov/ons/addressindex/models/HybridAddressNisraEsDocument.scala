package uk.gov.ons.addressindex.models

import org.apache.spark.sql.Row

case class HybridAddressNisraEsDocument(uprn: Long,
                                        postcodeIn: String,
                                        postcodeOut: String,
                                        parentUprn: Long,
                                        relatives: Seq[Map[String, Any]],
                                        lpi: Seq[Map[String, Any]],
                                        paf: Seq[Map[String, Any]],
                                        crossRefs: Seq[Map[String, String]],
                                        nisra: Seq[Map[String, Any]],
                                        classificationCode: Option[String],
                                        censusAddressType: String,
                                        censusEstabType: String,
                                        postcode: String,
                                        fromSource: String,
                                        countryCode: String,
                                        postcodeStreetTown: String)

object HybridAddressNisraEsDocument extends EsDocument with HybridAddress with NisraAddress {

  def rowToNisra(row: Row): Map[String, Any] = {

    val nisraFormatted: Array[String] = createNisraFormatted(row)

    Map(
      "uprn" -> row.getLong(0),
      "buildingNumber" -> toShort(row.getString(3)).orNull,
      "easting" -> row.getFloat(23),
      "northing" -> row.getFloat(24),
      "location" -> row.get(25),
      "creationDate" -> row.getDate(26),
      "commencementDate" -> row.getDate(27),
      "archivedDate" -> row.getDate(28),
      "buildingStatus" -> row.getString(29),
      "addressStatus" -> row.getString(30),
      "classificationCode" -> row.getString(31),
      "mixedNisra" -> nisraFormatted(0),
      "mixedNisraStart" -> nisraFormatted(0).replaceAll(",","").replaceAll("'","").take(12),
      "mixedAltNisra" -> nisraFormatted(1),
      "nisraAll" -> nisraFormatted(2),
      "organisationName" -> normalize(Option(row.getString(15)).getOrElse("")),
      "subBuildingName" -> normalize(Option(row.getString(1)).getOrElse("")),
      "buildingName" -> (normalize(Option(row.getString(2)).getOrElse("")) + buildingNameExtra(Option(row.getString(3)).getOrElse("1"))),
      "thoroughfare" -> normalize(Option(row.getString(16)).getOrElse("")),
      "altThoroughfare" -> normalize(Option(row.getString(17)).getOrElse("")),
      "dependentThoroughfare" -> normalize(Option(row.getString(18)).getOrElse("")),
      "locality" -> normalize(Option(row.getString(19)).getOrElse("")),
      "udprn" -> toInt(row.getString(20)).orNull,
      "townName" -> normalize(Option(row.getString(21)).getOrElse("")),
      "postcode" -> row.getString(22),
      "complete" -> row.getString(14),
      "paoText" -> normalize(Option(row.getString(8)).getOrElse("")),
      "paoStartNumber" -> toShort(row.getString(4)).orNull,
      "paoStartSuffix" -> row.getString(6),
      "paoEndNumber" -> toShort(row.getString(5)).orNull,
      "paoEndSuffix" -> row.getString(7),
      "saoText" -> normalize(Option(row.getString(13)).getOrElse("")),
      "saoStartNumber" -> toShort(row.getString(9)).orNull,
      "saoStartSuffix" -> row.getString(11),
      "saoEndNumber" -> toShort(row.getString(10)).orNull,
      "saoEndSuffix" -> row.getString(12),
      "secondarySort" -> addLeadingZeros(Option(row.getString(8)).getOrElse("") + " " + Option(row.getString(9)).getOrElse("") + Option(row.getString(11)).getOrElse("") + " " + Option(row.getString(13)).getOrElse("") + " " + Option(row.getString(15)).getOrElse("")).replaceAll(" +", " "),
      "localCouncil" -> row.getString(32),
      "LGDCode" -> nisraCouncilNameToCode(row.getString(32))
    )
  }

  private def buildingNameExtra(s: String): String = {
    try {
      val stest: Short = s.toShort
      ""
    } catch {
      case e: Exception => " " + s
    }
  }

}