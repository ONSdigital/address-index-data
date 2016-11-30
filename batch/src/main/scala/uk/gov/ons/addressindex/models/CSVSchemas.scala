package uk.gov.ons.addressindex.models

import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Contains schemas that should be applied to CSV documents
  */
object CSVSchemas {

  /**
    * Postcode address CSV file schema
    */
  val postcodeAddressFileSchema = StructType(Seq(
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
    StructField("welshPostTown", StringType, nullable = false),
    StructField("poBoxNumber", StringType, nullable = false),
    StructField("processDate", StringType, nullable = false),
    StructField("startDate", StringType, nullable = false),
    StructField("endDate", StringType, nullable = false),
    StructField("lastUpdateDate", StringType, nullable = false),
    StructField("entryDate", StringType, nullable = false)
  ))

  /**
    * BLPU CSV file schema
    */
  val blpuFileSchema = StructType(Seq(
    StructField("recordIdentifier", StringType, nullable = false),
    StructField("changeType", StringType, nullable = false),
    StructField("proOrder", StringType, nullable = false),
    StructField("uprn", StringType, nullable = false),
    StructField("logicalStatus", StringType, nullable = false),
    StructField("blpuState", StringType, nullable = false),
    StructField("blpuStateDate", StringType, nullable = false),
    StructField("parentUprn", StringType, nullable = false),
    StructField("xCoordinate", StringType, nullable = false),
    StructField("yCoordinate", StringType, nullable = false),
    StructField("latitude", StringType, nullable = false),
    StructField("longitude", StringType, nullable = false),
    StructField("rpc", StringType, nullable = false),
    StructField("localCustodianCode", StringType, nullable = false),
    StructField("country", StringType, nullable = false),
    StructField("startDate", StringType, nullable = false),
    StructField("endDate", StringType, nullable = false),
    StructField("lastUpdateDate", StringType, nullable = false),
    StructField("entryDate", StringType, nullable = false),
    StructField("addressbasePostal", StringType, nullable = false),
    StructField("postcodeLocator", StringType, nullable = false),
    StructField("multiOccCount", StringType, nullable = false)
  ))

  /**
    * Classification CSV file schema
    */
  val classificationFileSchema = StructType(Seq(
    StructField("recordIdentifier", StringType, nullable = false),
    StructField("changeType", StringType, nullable = false),
    StructField("proOrder", StringType, nullable = false),
    StructField("uprn", StringType, nullable = false),
    StructField("classKey", StringType, nullable = false),
    StructField("classificationCode", StringType, nullable = false),
    StructField("classScheme", StringType, nullable = false),
    StructField("schemeVersion", StringType, nullable = false),
    StructField("startDate", StringType, nullable = false),
    StructField("endDate", StringType, nullable = false),
    StructField("lastUpdateDate", StringType, nullable = false),
    StructField("entryDate", StringType, nullable = false)
  ))

  /**
    * crossref CSV file schema
    */
  val crossrefFileSchema = StructType(Seq(
    StructField("recordIdentifier", StringType, nullable = false),
    StructField("changeType", StringType, nullable = false),
    StructField("proOrder", StringType, nullable = false),
    StructField("uprn", StringType, nullable = false),
    StructField("xrefKey", StringType, nullable = false),
    StructField("crossReference", StringType, nullable = false),
    StructField("version", StringType, nullable = false),
    StructField("source", StringType, nullable = false),
    StructField("startDate", StringType, nullable = false),
    StructField("endDate", StringType, nullable = false),
    StructField("lastUpdateDate", StringType, nullable = false),
    StructField("entryDate", StringType, nullable = false)
  ))

  /**
    * lpi CSV file schema
    */
  val lpiFileSchema = StructType(Seq(
    StructField("recordIdentifier", StringType, nullable = false),
    StructField("changeType", StringType, nullable = false),
    StructField("proOrder", StringType, nullable = false),
    StructField("uprn", StringType, nullable = false),
    StructField("lpiKey", StringType, nullable = false),
    StructField("language", StringType, nullable = false),
    StructField("logicalStatus", StringType, nullable = false),
    StructField("startDate", StringType, nullable = false),
    StructField("endDate", StringType, nullable = false),
    StructField("lastUpdateDate", StringType, nullable = false),
    StructField("entryDate", StringType, nullable = false),
    StructField("saoStartNumber", StringType, nullable = false),
    StructField("saoStartSuffix", StringType, nullable = false),
    StructField("saoEndNumber", StringType, nullable = false),
    StructField("saoEndSuffix", StringType, nullable = false),
    StructField("saoText", StringType, nullable = false),
    StructField("paoStartNumber", StringType, nullable = false),
    StructField("paoStartSuffix", StringType, nullable = false),
    StructField("paoEndNumber", StringType, nullable = false),
    StructField("paoEndSuffix", StringType, nullable = false),
    StructField("paoText", StringType, nullable = false),
    StructField("usrn", StringType, nullable = false),
    StructField("usrnMatchIndicator", StringType, nullable = false),
    StructField("areaName", StringType, nullable = false),
    StructField("level", StringType, nullable = false),
    StructField("officialFlag", StringType, nullable = false)
  ))

  /**
    * organisation CSV file schema
    */
  val organisationFileSchema = StructType(Seq(
    StructField("recordIdentifier", StringType, nullable = false),
    StructField("changeType", StringType, nullable = false),
    StructField("proOrder", StringType, nullable = false),
    StructField("uprn", StringType, nullable = false),
    StructField("orgKey", StringType, nullable = false),
    StructField("organisation", StringType, nullable = false),
    StructField("legalName", StringType, nullable = false),
    StructField("startDate", StringType, nullable = false),
    StructField("endDate", StringType, nullable = false),
    StructField("lastUpdateDate", StringType, nullable = false),
    StructField("entryDate", StringType, nullable = false)
  ))

  /**
    * street CSV file schema
    */
  val streetFileSchema = StructType(Seq(
    StructField("recordIdentifier", StringType, nullable = false),
    StructField("changeType", StringType, nullable = false),
    StructField("proOrder", StringType, nullable = false),
    StructField("usrn", StringType, nullable = false),
    StructField("recordType", StringType, nullable = false),
    StructField("swaOrgRefNaming", StringType, nullable = false),
    StructField("state", StringType, nullable = false),
    StructField("stateDate", StringType, nullable = false),
    StructField("streetSurface", StringType, nullable = false),
    StructField("streetClassification", StringType, nullable = false),
    StructField("version", StringType, nullable = false),
    StructField("streetStartDate", StringType, nullable = false),
    StructField("streetEndDate", StringType, nullable = false),
    StructField("lastUpdateDate", StringType, nullable = false),
    StructField("recordEntryDate", StringType, nullable = false),
    StructField("streetStartX", StringType, nullable = false),
    StructField("streetStartY", StringType, nullable = false),
    StructField("streetStartLat", StringType, nullable = false),
    StructField("streetStartLong", StringType, nullable = false),
    StructField("streetEndX", StringType, nullable = false),
    StructField("streetEndY", StringType, nullable = false),
    StructField("streetEndLat", StringType, nullable = false),
    StructField("streetEndLong", StringType, nullable = false),
    StructField("streetTolerance", StringType, nullable = false)
  ))

  /**
    * street-descriptor CSV file schema
    */
  val streetDescriptorFileSchema = StructType(Seq(
    StructField("recordIdentifier", StringType, nullable = false),
    StructField("changeType", StringType, nullable = false),
    StructField("proOrder", StringType, nullable = false),
    StructField("usrn", StringType, nullable = false),
    StructField("streetDescriptor", StringType, nullable = false),
    StructField("locality", StringType, nullable = false),
    StructField("townName", StringType, nullable = false),
    StructField("administrativeArea", StringType, nullable = false),
    StructField("language", StringType, nullable = false),
    StructField("startDate", StringType, nullable = false),
    StructField("endDate", StringType, nullable = false),
    StructField("lastUpdateDate", StringType, nullable = false),
    StructField("entryDate", StringType, nullable = false)
  ))

  /**
    * successor CSV file schema
    */
  val successorFileSchema = StructType(Seq(
    StructField("recordIdentifier", StringType, nullable = false),
    StructField("changeType", StringType, nullable = false),
    StructField("proOrder", StringType, nullable = false),
    StructField("uprn", StringType, nullable = false),
    StructField("succKey", StringType, nullable = false),
    StructField("startDate", StringType, nullable = false),
    StructField("endDate", StringType, nullable = false),
    StructField("lastUpdateDate", StringType, nullable = false),
    StructField("entryDate", StringType, nullable = false),
    StructField("successor", StringType, nullable = false)
  ))
}