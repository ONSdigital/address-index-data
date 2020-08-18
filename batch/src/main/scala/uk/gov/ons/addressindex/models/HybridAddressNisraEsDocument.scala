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
      "organisationName" -> normalize(Option(row.getString(15)).getOrElse("")),
      "subBuildingName" -> normalize(Option(row.getString(1)).getOrElse("")),
      "buildingName" -> (normalize(Option(row.getString(2)).getOrElse("")) + buildingNameExtra(Option(row.getString(3)).getOrElse("1"))),
      "thoroughfare" -> normalize(Option(row.getString(16)).getOrElse("")),
      "altThoroughfare" -> normalize(Option(row.getString(17)).getOrElse("")),
      "dependentThoroughfare" -> normalize(Option(row.getString(18)).getOrElse("")),
      "locality" -> normalize(Option(row.getString(19)).getOrElse("")),
      "udprn" -> (if (row.isNullAt(20)) null else row.getInt(20)),
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
      "townland" -> normalize(Option(row.getString(32)).getOrElse("")),
      "county" -> normalize(Option(row.getString(33)).getOrElse("")),
      "localCustodianCode" -> Option(row.getString(34)).getOrElse(""),
      "blpuState" -> (if (row.isNullAt(35)) null else row.getByte(35)),
      "logicalStatus" -> (if (row.isNullAt(36)) null else row.getByte(36)),
      "addressType" -> Option(row.getString(37)).getOrElse(""),
      "estabType" -> Option(row.getString(38)).getOrElse(""),
      "lad" -> Option(row.getString(39)).getOrElse(""),
      "region" -> normalize(Option(row.getString(40)).getOrElse("")),
      "recordIdentifier" -> (if (row.isNullAt(41)) null else row.getByte(41)),
      "parentUprn" -> (if (row.isNullAt(42)) null else row.getLong(42)),
      "usrn" -> (if (row.isNullAt(43)) null else row.getInt(43)),
      "primaryUprn" -> (if (row.isNullAt(44)) null else row.getLong(44)),
      "secondaryUprn" -> Option(row.getString(45)).getOrElse(""),
      "thisLayer" ->  (if (row.isNullAt(46)) null else row.getInt(46)),
      "layers" -> (if (row.isNullAt(47)) null else row.getInt(47)),
      "nodeType" -> (Option(row.getString(48)).getOrElse("")),
      "addressLine1" -> normalize(Option(row.getString(49)).getOrElse("")),
      "addressLine2" -> normalize(Option(row.getString(50)).getOrElse("")),
      "addressLine3" -> normalize(Option(row.getString(51)).getOrElse("")),
      "tempCoords" -> Option(row.getString(52)).getOrElse(""),
      "address1YearAgo" -> normalize(Option(row.getString(53)).getOrElse("")),
      "classification" -> Option(row.getString(54)).getOrElse(""),
      "postTown" -> normalize(Option(row.getString(55)).getOrElse(""))
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