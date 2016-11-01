package uk.gov.ons.addressindex.models

import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Contains schemas that should be applied to CSV documents
  */
object CSVSchemas {

  /**
    * Delivery point CSV file schema
    */
  val deliveryPointSchema = StructType(Seq(
    StructField("recordIdentifier", StringType, nullable = false),
    StructField("changeType", StringType, nullable = false),
    StructField("proOrder", StringType, nullable = false),
    StructField("uprn", StringType, nullable = false),
    StructField("udprn", StringType, nullable = false),
    StructField("organizationName", StringType, nullable = false),
    StructField("departmentName", StringType, nullable = false),
    StructField("subBuildingName", StringType, nullable = false),
    StructField("buildingName", StringType, nullable = false),
    StructField("buildingNumber", StringType, nullable = false),
    StructField("dependentThoroughfare", StringType, nullable = false),
    StructField("thoroughfare", StringType, nullable = false),
    StructField("doubleDependentLocality", StringType, nullable = false),
    StructField("dependentLocality", StringType, nullable = false),
    StructField("postTown", StringType, nullable = false),
    StructField("postcode", StringType, nullable = false),
    StructField("postcodeType", StringType, nullable = false),
    StructField("deliveryPointSuffix", StringType, nullable = false),
    StructField("welshDependentThoroughfare", StringType, nullable = false),
    StructField("welshThoroughfare", StringType, nullable = false),
    StructField("welshDoubleDependentLocality", StringType, nullable = false),
    StructField("welshDependentLocality", StringType, nullable = false),
    StructField("welshDependentLocality", StringType, nullable = false),
    StructField("welshPostTown", StringType, nullable = false),
    StructField("poBoxNumber", StringType, nullable = false),
    StructField("processDate", StringType, nullable = false),
    StructField("startDate", StringType, nullable = false),
    StructField("endDate", StringType, nullable = false),
    StructField("lastUpdateDate", StringType, nullable = false),
    StructField("entryDate", StringType, nullable = false)
  ))


}
