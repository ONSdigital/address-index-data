package uk.gov.ons.addressindex.models

import org.apache.spark.sql.types._

object NisraSchema {

  val nisraFileSchema = StructType(Seq(
    StructField("uprn", StringType, nullable = true),
    StructField("subBuildingName", StringType, nullable = true),
    StructField("buildingName", StringType, nullable = true),
    StructField("buildingNumber", StringType, nullable = true),
    StructField("paoStartNumber", StringType, nullable = true),
    StructField("paoEndNumber", StringType, nullable = true),
    StructField("paoStartSuffix", StringType, nullable = true),
    StructField("paoEndSuffix", StringType, nullable = true),
    StructField("paoText", StringType, nullable = true),
    StructField("saoStartNumber", StringType, nullable = true),
    StructField("saoEndNumber", StringType, nullable = true),
    StructField("saoStartSuffix", StringType, nullable = true),
    StructField("saoText", StringType, nullable = true),
    StructField("saoEndSuffix", StringType, nullable = true),
    StructField("complete", StringType, nullable = true),
    StructField("organisationName", StringType, nullable = true),
    StructField("primaryThorfare", StringType, nullable = true),
    StructField("secondaryThorfare", StringType, nullable = true),
    StructField("postTown", StringType, nullable = true),
    StructField("postcode", StringType, nullable = true)
  ))

//  val nisraFileSchema = StructType(Seq(
//    StructField("organisationName", StringType, nullable = true),
//    StructField("subBuildingName", StringType, nullable = true),
//    StructField("buildingName", StringType, nullable = true),
//    StructField("buildingNumber", StringType, nullable = true),
//    StructField("thoroughfare", StringType, nullable = true),
//    StructField("altThoroughfare", StringType, nullable = true),
//    StructField("dependentThoroughfare", StringType, nullable = true),
//    StructField("locality", StringType, nullable = true),
//    StructField("townland", StringType, nullable = true),
//    StructField("townName", StringType, nullable = true),
//    StructField("county", StringType, nullable = true),
//    StructField("postcode", StringType, nullable = true),
//    StructField("uprn", StringType, nullable = true),
//    StructField("localCouncil", StringType, nullable = true),
//    StructField("buildingStatus", StringType, nullable = true),
//    StructField("addressStatus", StringType, nullable = true),
//    StructField("classificationCode", StringType, nullable = true),
//    StructField("udprn", StringType, nullable = true),
//    StructField("postTown", StringType, nullable = true),
//    StructField("sa2011", StringType, nullable = true),
//    StructField("coa2011", StringType, nullable = true),
//    StructField("lgd2014", StringType, nullable = true),
//    StructField("ward2014", StringType, nullable = true),
//    StructField("soa2001", StringType, nullable = true),
//    StructField("ward1992", StringType, nullable = true),
//    StructField("lgd1992", StringType, nullable = true),
//    StructField("aa1998", StringType, nullable = true),
//    StructField("aa2008", StringType, nullable = true),
//    StructField("lgd1984", StringType, nullable = true),
//    StructField("ward1984", StringType, nullable = true),
//    StructField("ed1991Map", StringType, nullable = true),
//    StructField("ed1991", StringType, nullable = true),
//    StructField("settlement", StringType, nullable = true),
//    StructField("settlementBand", StringType, nullable = true),
//    StructField("nraCode", StringType, nullable = true),
//    StructField("ttwa2007", StringType, nullable = true),
//    StructField("blpu", StringType, nullable = true),
//    StructField("uniqueBuildingId", StringType, nullable = true), // Parent UPRN?
//    StructField("usrn", StringType, nullable = true),
//    StructField("xCoordinate", StringType, nullable = true),
//    StructField("yCoordinate", StringType, nullable = true),
//    StructField("tempCoords", StringType, nullable = true),
//    StructField("creationDate", StringType, nullable = true),
//    StructField("commencementDate", StringType, nullable = true),
//    StructField("archivedDate", StringType, nullable = true),
//    StructField("action", StringType, nullable = true),
//    StructField("latitude", StringType, nullable = true),
//    StructField("longitude", StringType, nullable = true)
//  ))
}

