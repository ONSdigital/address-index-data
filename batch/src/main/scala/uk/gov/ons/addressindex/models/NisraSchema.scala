package uk.gov.ons.addressindex.models

import org.apache.spark.sql.types._

object NisraSchema {

  val nisraFileSchema = StructType(Seq(
    StructField("organisationName", StringType, nullable = true),
    StructField("subBuildingName", StringType, nullable = true),
    StructField("buildingName", StringType, nullable = true),
    StructField("buildingNumber", ShortType, nullable = true),
    StructField("thoroughfare", StringType, nullable = true),
    StructField("dependentThoroughfare", StringType, nullable = true),
    StructField("altThoroughfare", StringType, nullable = true),
    StructField("locality", StringType, nullable = true),
    StructField("townland", StringType, nullable = true),
    StructField("townName", StringType, nullable = true),
    StructField("postcode", StringType, nullable = false),
    StructField("uprn", LongType, nullable = false),
    StructField("buildingStatus", StringType, nullable = false),
    StructField("addressStatus", StringType, nullable = false),
    StructField("classificationCode", StringType, nullable = false),
    StructField("udprn", IntegerType, nullable = false),
    StructField("postTown", StringType, nullable = false),
    StructField("uniqueBuildingId", LongType, nullable = false), // Parent UPRN?
    StructField("usrn", IntegerType, nullable = false),
    StructField("xCoordinate", FloatType, nullable = false),
    StructField("yCoordinate", FloatType, nullable = false),
    StructField("creationDate", DateType, nullable = true),
    StructField("commencementDate", DateType, nullable = true),
    StructField("archivedDate", DateType, nullable = true),
    StructField("latitude", FloatType, nullable = false),
    StructField("longitude", FloatType, nullable = false)
  ))
}
