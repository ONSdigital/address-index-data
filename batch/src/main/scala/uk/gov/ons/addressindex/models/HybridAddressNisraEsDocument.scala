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
                                        postcodeStreetTown: String,
                                        postTown: String)

object HybridAddressNisraEsDocument extends EsDocument with HybridAddress with NisraAddress {

  def rowToNisra(row: Row): Map[String, Any] = {

    val nisraFormatted: Array[String] = createNisraFormatted(row)

    Map(
      "uprn" -> row.getLong(0),
      "buildingNumber" -> toShort(row.getString(3)).orNull,
      "easting" -> row.getFloat(22),
      "northing" -> row.getFloat(23),
      "location" -> row.get(24),
      "creationDate" -> row.getDate(25),
      "commencementDate" -> row.getDate(26),
      "archivedDate" -> row.getDate(27),
      "classificationCode" -> row.getString(28),
      "mixedNisra" -> nisraFormatted(0),
      "mixedNisraStart" -> nisraFormatted(0).replaceAll(",","").replaceAll("'","").take(12),
      "mixedAltNisra" -> nisraFormatted(1),
      "nisraAll" -> nisraFormatted(2),
      "organisationName" -> normalize(Option(row.getString(14)).getOrElse("")),
      "subBuildingName" -> normalize(Option(row.getString(1)).getOrElse("")),
      "buildingName" -> (normalize(Option(row.getString(2)).getOrElse("")) + buildingNameExtra(Option(row.getString(3)).getOrElse("1"))),
      "thoroughfare" -> normalize(Option(row.getString(15)).getOrElse("")),
      "altThoroughfare" -> normalize(Option(row.getString(16)).getOrElse("")),
      "dependentThoroughfare" -> normalize(Option(row.getString(17)).getOrElse("")),
      "locality" -> normalize(Option(row.getString(18)).getOrElse("")),
      "udprn" -> (if (row.isNullAt(19)) null else row.getInt(19)),
      "townName" -> normalize(Option(row.getString(20)).getOrElse("")),
      "postcode" -> row.getString(21),
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
      "secondarySort" -> addLeadingZeros(Option(row.getString(8)).getOrElse("") + " " + Option(row.getString(9)).getOrElse("") + Option(row.getString(11)).getOrElse("") + " " + Option(row.getString(13)).getOrElse("") + " " + Option(row.getString(14)).getOrElse("")).replaceAll(" +", " "),
      "townland" -> normalize(Option(row.getString(29)).getOrElse("")),
      "county" -> normalize(Option(row.getString(30)).getOrElse("")),
      "localCustodianCode" -> Option(row.getString(31)).getOrElse(""),
      "blpuState" -> (if (row.isNullAt(32)) null else row.getByte(32)),
      "logicalStatus" -> (if (row.isNullAt(33)) null else row.getByte(33)),
      "addressType" -> Option(row.getString(34)).getOrElse(""),
      "estabType" -> Option(row.getString(35)).getOrElse(""),
      "lad" -> Option(row.getString(36)).getOrElse(""),
      "region" -> normalize(Option(row.getString(37)).getOrElse("")),
      "recordIdentifier" -> (if (row.isNullAt(38)) null else row.getByte(38)),
      "parentUprn" -> (if (row.isNullAt(39)) null else row.getLong(39)),
      "usrn" -> (if (row.isNullAt(40)) null else row.getInt(40)),
      "primaryUprn" -> (if (row.isNullAt(41)) null else row.getLong(41)),
      "secondaryUprn" -> Option(row.getString(42)).getOrElse(""),
      "thisLayer" ->  (if (row.isNullAt(43)) null else row.getInt(43)),
      "layers" -> (if (row.isNullAt(44)) null else row.getInt(44)),
      "nodeType" -> (Option(row.getString(45)).getOrElse("")),
      "addressLine1" -> normalize(Option(row.getString(46)).getOrElse("")),
      "addressLine2" -> normalize(Option(row.getString(47)).getOrElse("")),
      "addressLine3" -> normalize(Option(row.getString(48)).getOrElse("")),
      "tempCoords" -> Option(row.getString(49)).getOrElse(""),
      "address1YearAgo" -> normalize(Option(row.getString(50)).getOrElse("")),
      "postTown" -> normalize(Option(row.getString(51)).getOrElse(""))
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