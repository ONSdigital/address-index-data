package uk.gov.ons.addressindex.models

import org.apache.spark.sql.Row
import uk.gov.ons.addressindex.models.HybridAddressSkinnyNisraEsDocument.{normalize, normalizeTowns, startsWithNumber}

trait NisraAddress {

  def generateFormattedNisraAddresses(organisationName: String, subBuildingName: String, buildingName: String, buildingNumber: String, thoroughfare: String,
                                      altThoroughfare: String, dependentThoroughfare: String, locality: String, townland: String, townName: String, postTown: String,
                                      postcode: String) : Array[String] = {

    val trimmedSubBuildingName = normalize(subBuildingName)
    val trimmedBuildingName = normalize(buildingName)
    val trimmedThoroughfare = normalize(thoroughfare)
    val trimmedAltThoroughfare = normalize(altThoroughfare)
    val trimmedDependentThoroughfare = normalize(dependentThoroughfare)

    val buildingNumberWithStreetDescription = s"${buildingNumber.toUpperCase} $trimmedThoroughfare"
    val buildingNameWithStreetDescription = s"${trimmedBuildingName.toUpperCase} $trimmedThoroughfare"
    val commalessNumberAndStreetPart = if (startsWithNumber.findFirstIn(trimmedBuildingName).isDefined) buildingNameWithStreetDescription else buildingNumberWithStreetDescription

    val postTownExtra = if (postTown.equals(townName)) "" else postTown

    Array(
      Seq(normalize(organisationName),
        trimmedSubBuildingName,
        if (startsWithNumber.findFirstIn(trimmedBuildingName).isDefined) "" else trimmedBuildingName,
        commalessNumberAndStreetPart,
        trimmedDependentThoroughfare,
        normalizeTowns(locality),
        normalizeTowns(townland),
        normalizeTowns(townName),
        normalizeTowns(postTownExtra),
        postcode.toUpperCase).map(_.trim).filter(_.nonEmpty).mkString(", "),
      if (!altThoroughfare.isEmpty)
        Seq(normalize(organisationName),
          trimmedSubBuildingName,
          trimmedBuildingName,
          buildingNumber,
          trimmedAltThoroughfare,
          trimmedDependentThoroughfare,
          normalizeTowns(locality),
          normalizeTowns(townland),
          normalizeTowns(townName),
          normalizeTowns(postTownExtra),
          postcode.toUpperCase).map(_.trim).filter(_.nonEmpty).mkString(", ") else "",
      Seq(organisationName,
        subBuildingName,
        buildingName,
        buildingNumber,
        thoroughfare,
        dependentThoroughfare,
        altThoroughfare,
        locality,
        townland,
        townName,
        postTownExtra,
        postcode).map(_.trim).filter(_.nonEmpty).mkString(" ")
    )
  }

  protected def createNisraFormatted(row: Row): Array[String] = generateFormattedNisraAddresses(
      Option(row.getString(14)).getOrElse(""),
      Option(row.getString(1)).getOrElse(""),
      Option(row.getString(2)).getOrElse(""),
      Option(row.getString(3)).getOrElse(""),
      Option(row.getString(15)).getOrElse(""),
      Option(row.getString(16)).getOrElse(""),
      Option(row.getString(17)).getOrElse(""),
      Option(row.getString(18)).getOrElse(""),
      "",
      Option(row.getString(20)).getOrElse(""),
      Option(row.getString(51)).getOrElse(""),
      Option(row.getString(21)).getOrElse("") + " " + Option(row.getString(21)).getOrElse("").replace(" ","")
    )

}