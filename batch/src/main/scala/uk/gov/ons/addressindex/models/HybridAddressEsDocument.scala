package uk.gov.ons.addressindex.models

import org.apache.spark.sql.Row

case class HybridAddressEsDocument(
  uprn: String,
  lpi: Seq[Map[String, String]],
  paf: Seq[Map[String, String]]
)

object HybridAddressEsDocument {

  def rowToLpi(row: Row): Map[String, String] = Map(
    "uprn" -> row.getString(0),
    "postcodeLocator" -> row.getString(1),
    "addressBasePostal" -> row.getString(2),
    "latitude" -> row.getString(3),
    "longitude" -> row.getString(4),
    "easting" -> row.getString(5),
    "northing" -> row.getString(6),
    "organisation" -> row.getString(7),
    "legalName" -> row.getString(8),
    "classificationCode" -> row.getString(9),
    "usrn" -> row.getString(10),
    "lpiKey" -> row.getString(11),
    "paoText" -> row.getString(12),
    "paoStartNumber" -> row.getString(13),
    "paoStartSuffix" -> row.getString(14),
    "paoEndNumber" -> row.getString(15),
    "paoEndSuffix" -> row.getString(16),
    "saoText" -> row.getString(17),
    "saoStartNumber" -> row.getString(18),
    "saoStartSuffix" -> row.getString(19),
    "saoEndNumber" -> row.getString(20),
    "saoEndSuffix" -> row.getString(21),
    "level" -> row.getString(22),
    "officialFlag" -> row.getString(23),
    "logicalStatus" -> row.getString(24),
    "streetDescriptor" -> row.getString(25),
    "townName" -> row.getString(26),
    "locality" -> row.getString(27)
  )

  def rowToPaf(row: Row): Map[String, String] = Map(
    "recordIdentifier" -> row.getString(0),
    "changeType" -> row.getString(1),
    "proOrder" -> row.getString(2),
    "uprn" -> row.getString(3),
    "udprn" -> row.getString(4),
    "organizationName" -> row.getString(5),
    "departmentName" -> row.getString(6),
    "subBuildingName" -> row.getString(7),
    "buildingName" -> row.getString(8),
    "buildingNumber" -> row.getString(9),
    "dependentThoroughfare" -> row.getString(10),
    "thoroughfare" -> row.getString(11),
    "doubleDependentLocality" -> row.getString(12),
    "dependentLocality" -> row.getString(13),
    "postTown" -> row.getString(14),
    "postcode" -> row.getString(15),
    "postcodeType" -> row.getString(16),
    "deliveryPointSuffix" -> row.getString(17),
    "welshDependentThoroughfare" -> row.getString(18),
    "welshThoroughfare" -> row.getString(19),
    "welshDoubleDependentLocality" -> row.getString(20),
    "welshDependentLocality" -> row.getString(21),
    "welshPostTown" -> row.getString(22),
    "poBoxNumber" -> row.getString(23),
    "processDate" -> row.getString(24),
    "startDate" -> row.getString(25),
    "endDate" -> row.getString(26),
    "lastUpdateDate" -> row.getString(27),
    "entryDate" -> row.getString(28)
  )
}
