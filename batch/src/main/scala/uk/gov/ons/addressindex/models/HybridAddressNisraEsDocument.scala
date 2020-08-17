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
      "organisationName" -> (Option(row.getString(15)).getOrElse("")),
      "subBuildingName" -> (Option(row.getString(1)).getOrElse("")),
      "buildingName" -> ((Option(row.getString(2)).getOrElse("")) + buildingNameExtra(Option(row.getString(3)).getOrElse("1"))),
      "thoroughfare" -> (Option(row.getString(16)).getOrElse("")),
      "altThoroughfare" -> (Option(row.getString(17)).getOrElse("")),
      "dependentThoroughfare" -> (Option(row.getString(18)).getOrElse("")),
      "locality" -> (Option(row.getString(19)).getOrElse("")),
      "udprn" -> toInt(row.getString(20)).orNull,
      "townName" -> (Option(row.getString(21)).getOrElse("")),
      "postcode" -> row.getString(22),
      "complete" -> row.getString(14),
      "paoText" -> (Option(row.getString(8)).getOrElse("")),
      "paoStartNumber" -> toShort(row.getString(4)).orNull,
      "paoStartSuffix" -> row.getString(6),
      "paoEndNumber" -> toShort(row.getString(5)).orNull,
      "paoEndSuffix" -> row.getString(7),
      "saoText" -> (Option(row.getString(13)).getOrElse("")),
      "saoStartNumber" -> toShort(row.getString(9)).orNull,
      "saoStartSuffix" -> row.getString(11),
      "saoEndNumber" -> toShort(row.getString(10)).orNull,
      "saoEndSuffix" -> row.getString(12),
      "secondarySort" -> addLeadingZeros(Option(row.getString(8)).getOrElse("") + " " + Option(row.getString(9)).getOrElse("") + Option(row.getString(11)).getOrElse("") + " " + Option(row.getString(13)).getOrElse("") + " " + Option(row.getString(15)).getOrElse("")).replaceAll(" +", " "),
      "townland" -> (Option(row.getString(32)).getOrElse("")),
      "county" -> (Option(row.getString(33)).getOrElse("")),
      "localCustodianCode" -> (Option(row.getString(34)).getOrElse("")),
      "blpuState" -> (Option(row.getString(35)).getOrElse("")),
      "logicalStatus" -> (Option(row.getString(36)).getOrElse("")),
      "udprn" -> (Option(row.getString(37)).getOrElse("")),
      "postTown" -> (Option(row.getString(38)).getOrElse("")),
      "addressType" -> (Option(row.getString(39)).getOrElse("")),
      "estabType" -> (Option(row.getString(40)).getOrElse("")),
      "oa11" -> (Option(row.getString(41)).getOrElse("")),
      "lsoa11" -> (Option(row.getString(42)).getOrElse("")),
      "msoa11" -> (Option(row.getString(43)).getOrElse("")),
      "lad" -> (Option(row.getString(44)).getOrElse("")),
      "oa11" -> (Option(row.getString(45)).getOrElse("")),
      "region" -> (Option(row.getString(46)).getOrElse("")),
      "recordIdentifier" -> (Option(row.getString(47)).getOrElse("")),
      "parentUprn" -> (Option(row.getString(48)).getOrElse("")),
      "usrn" -> (Option(row.getString(49)).getOrElse("")),
      "xCoordinate" -> (Option(row.getString(50)).getOrElse("")),
      "yCoordinate" -> (Option(row.getString(51)).getOrElse("")),
      "primaryUprn" -> (Option(row.getString(52)).getOrElse("")),
      "secondaryUprn" -> (Option(row.getString(53)).getOrElse("")),
      "thisLayer" -> (Option(row.getString(54)).getOrElse("")),
      "layers" -> (Option(row.getString(55)).getOrElse("")),
      "nodeType" -> (Option(row.getString(56)).getOrElse("")),
      "latitude" -> (Option(row.getString(57)).getOrElse("")),
      "longitude" -> (Option(row.getString(58)).getOrElse("")),
      "addressLine1" -> (Option(row.getString(59)).getOrElse("")),
      "addressLine2" -> (Option(row.getString(60)).getOrElse("")),
      "addressLine3" -> (Option(row.getString(61)).getOrElse("")),
      "tempCoords" -> (Option(row.getString(62)).getOrElse("")),
      "WARD2014" -> (Option(row.getString(63)).getOrElse("")),
      "SOA2001" -> (Option(row.getString(64)).getOrElse("")),
      "WARD1992" -> (Option(row.getString(65)).getOrElse("")),
      "LGD1992" -> (Option(row.getString(66)).getOrElse("")),
      "AA1998" -> (Option(row.getString(67)).getOrElse("")),
      "AA2008" -> (Option(row.getString(68)).getOrElse("")),
      "LGD1984" -> (Option(row.getString(69)).getOrElse("")),
      "WARD1984" -> (Option(row.getString(70)).getOrElse("")),
      "ED1991_MAP" -> (Option(row.getString(71)).getOrElse("")),
      "ED1991" -> (Option(row.getString(72)).getOrElse("")),
      "settlement" -> (Option(row.getString(73)).getOrElse("")),
      "settlementband" -> (Option(row.getString(74)).getOrElse("")),
      "NRA_CODE" -> (Option(row.getString(75)).getOrElse("")),
      "TTWA2007" -> (Option(row.getString(76)).getOrElse("")),
      "address1YearAgo" -> (Option(row.getString(77)).getOrElse("")),
      "buildingStatus" -> (Option(row.getString(78)).getOrElse("")),
      "classification" -> (Option(row.getString(79)).getOrElse("")),
      "complete" -> (Option(row.getString(80)).getOrElse(""))
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