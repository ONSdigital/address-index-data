package uk.gov.ons.addressindex.models

import org.apache.spark.sql.Row
import uk.gov.ons.addressindex.utils.SparkProvider

case class HybridAddressEsDocument(
  uprn: Long,
  postcodeIn: String,
  postcodeOut: String,
  lpi: Seq[Map[String, Any]],
  paf: Seq[Map[String, Any]]
)

object HybridAddressEsDocument {

  def rowToLpi(row: Row): Map[String, Any] = Map(
    "uprn" -> row.getLong(0),
    "postcodeLocator" -> row.getString(1),
    "addressBasePostal" -> row.getString(2),
    "location" -> row.get(3),
    "easting" -> row.getFloat(4),
    "northing" -> row.getFloat(5),
    "parentUprn" -> (if (row.isNullAt(6)) null else row.getLong(6)),
    "multiOccCount" -> row.getShort(7),
    "blpuLogicalStatus" -> row.getByte(8),
    "localCustodianCode" -> row.getShort(9),
    "rpc" -> row.getByte(10),
    "organisation" -> row.getString(11),
    "legalName" -> row.getString(12),
    "classScheme" -> row.getString(13),
    "classificationCode" -> row.getString(14),
    "usrn" -> row.getInt(15),
    "lpiKey" -> row.getString(16),
    "paoText" -> row.getString(17),
    "paoStartNumber" -> (if (row.isNullAt(18)) null else row.getShort(18)),
    "paoStartSuffix" -> row.getString(19),
    "paoEndNumber" -> (if (row.isNullAt(20)) null else row.getShort(20)),
    "paoEndSuffix" -> row.getString(21),
    "saoText" -> row.getString(22),
    "saoStartNumber" -> (if (row.isNullAt(23)) null else row.getShort(23)),
    "saoStartSuffix" -> row.getString(24),
    "saoEndNumber" -> (if (row.isNullAt(25)) null else row.getShort(25)),
    "saoEndSuffix" -> row.getString(26),
    "level" -> row.getString(27),
    "officialFlag" -> row.getString(28),
    "lpiLogicalStatus" -> row.getByte(29),
    "usrnMatchIndicator" -> row.getByte(30),
    "language" -> row.getString(31),
    "streetDescriptor" -> row.getString(32),
    "townName" -> row.getString(33),
    "locality" -> row.getString(34),
    "streetClassification" -> (if (row.isNullAt(35)) null else row.getByte(35)),
    "crossReference" -> row.getString(36),
    "source" -> row.getString(37),
    "relatives" -> row.get(38),
    "nagAll" -> row.getString(39),
    "lpiStartDate" -> row.getDate(40),
    "lpiLastUpdateDate" -> row.getDate(41)
  )

  def rowToPaf(row: Row): Map[String, Any] = Map(
    "recordIdentifier" -> row.getByte(0),
    "changeType" -> row.getString(1),
    "proOrder" -> row.getLong(2),
    "uprn" -> row.getLong(3),
    "udprn" -> row.getInt(4),
    "organisationName" -> row.getString(5),
    "departmentName" -> row.getString(6),
    "subBuildingName" -> row.getString(7),
    "buildingName" -> row.getString(8),
    "buildingNumber" -> (if (row.isNullAt(9)) null else row.getShort(9)),
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
    "processDate" -> row.getDate(24),
    "startDate" -> row.getDate(25),
    "endDate" -> row.getDate(26),
    "lastUpdateDate" -> row.getDate(27),
    "entryDate" -> row.getDate(28),
    "pafAll" -> concatPaf(row.getString(23), if (row.isNullAt(9)) "" else row.getShort(9).toString, row.getString(10),
      row.getString(18), row.getString(11), row.getString(19), row.getString(6), row.getString(5), row.getString(7), row.getString(8),
      row.getString(12), row.getString(20), row.getString(13), row.getString(21), row.getString(14), row.getString(22), row.getString(15))
  )

  def concatPaf(poBoxNumber: String, buildingNumber: String, dependentThoroughfare: String, welshDependentThoroughfare:
  String, thoroughfare: String, welshThoroughfare: String, departmentName: String, organisationName: String,
    subBuildingName: String, buildingName: String, doubleDependentLocality: String,
    welshDoubleDependentLocality: String, dependentLocality: String, welshDependentLocality: String,
    postTown: String, welshPostTown: String, postcode: String): String = {

    val langDependentThoroughfare = if (dependentThoroughfare == welshDependentThoroughfare)
      s"$dependentThoroughfare" else s"$dependentThoroughfare $welshDependentThoroughfare"

    val langThoroughfare = if (thoroughfare == welshThoroughfare)
      s"$thoroughfare" else s"$thoroughfare $welshThoroughfare"

    val langDoubleDependentLocality = if (doubleDependentLocality == welshDoubleDependentLocality)
      s"$doubleDependentLocality" else s"$doubleDependentLocality $welshDoubleDependentLocality"

    val langDependentLocality = if (dependentLocality == welshDependentLocality)
      s"$dependentLocality" else s"$dependentLocality $welshDependentLocality"

    val langPostTown = if (postTown == welshPostTown)
      s"$postTown" else s"$postTown $welshPostTown"

    val buildingNumberWithStreetName =
      s"$buildingNumber ${if (langDependentThoroughfare.nonEmpty) s"$langDependentThoroughfare " else ""}$langThoroughfare"

    Seq(departmentName, organisationName, subBuildingName, buildingName,
      poBoxNumber, buildingNumberWithStreetName, langDoubleDependentLocality, langDependentLocality,
      langPostTown, postcode).map(_.trim).filter(_.nonEmpty).mkString(" ")
  }

}
