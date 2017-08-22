package uk.gov.ons.addressindex.models

import org.apache.spark.sql.types._
import org.elasticsearch.hadoop.serialization.dto.mapping.GeoPointType

/**
  * Contains schemas that should be applied to CSV documents
  */
object CSVSchemas {

  /**
    * Postcode address CSV file schema
    */
  val postcodeAddressFileSchema = StructType(Seq(
    StructField("recordIdentifier", ByteType, nullable = false),
    StructField("changeType", StringType, nullable = false),
    StructField("proOrder", LongType, nullable = false),
    StructField("uprn", LongType, nullable = false),
    StructField("udprn", IntegerType, nullable = false),
    StructField("organisationName", StringType, nullable = true),
    StructField("departmentName", StringType, nullable = true),
    StructField("subBuildingName", StringType, nullable = true),
    StructField("buildingName", StringType, nullable = true),
    StructField("buildingNumber", ShortType, nullable = true),
    StructField("dependentThoroughfare", StringType, nullable = true),
    StructField("thoroughfare", StringType, nullable = true),
    StructField("doubleDependentLocality", StringType, nullable = true),
    StructField("dependentLocality", StringType, nullable = true),
    StructField("postTown", StringType, nullable = false),
    StructField("postcode", StringType, nullable = false),
    StructField("postcodeType", StringType, nullable = false),
    StructField("deliveryPointSuffix", StringType, nullable = false),
    StructField("welshDependentThoroughfare", StringType, nullable = true),
    StructField("welshThoroughfare", StringType, nullable = true),
    StructField("welshDoubleDependentLocality", StringType, nullable = true),
    StructField("welshDependentLocality", StringType, nullable = true),
    StructField("welshPostTown", StringType, nullable = true),
    StructField("poBoxNumber", StringType, nullable = true),
    StructField("processDate", DateType, nullable = false),
    StructField("startDate", DateType, nullable = false),
    StructField("endDate", DateType, nullable = true),
    StructField("lastUpdateDate", DateType, nullable = false),
    StructField("entryDate", DateType, nullable = false)
  ))

  /**
    * BLPU CSV file schema
    */
  val blpuFileSchema = StructType(Seq(
    StructField("recordIdentifier", ByteType, nullable = false),
    StructField("changeType", StringType, nullable = false),
    StructField("proOrder", LongType, nullable = false),
    StructField("uprn", LongType, nullable = false),
    StructField("logicalStatus", ByteType, nullable = false),
    StructField("blpuState", ByteType, nullable = true),
    StructField("blpuStateDate", DateType, nullable = true),
    StructField("parentUprn", LongType, nullable = true),
    StructField("xCoordinate", FloatType, nullable = false),
    StructField("yCoordinate", FloatType, nullable = false),
    StructField("latitude", FloatType, nullable = false),
    StructField("longitude", FloatType, nullable = false),
    StructField("rpc", ByteType, nullable = false),
    StructField("localCustodianCode", ShortType, nullable = false),
    StructField("country", StringType, nullable = false),
    StructField("startDate", DateType, nullable = false),
    StructField("endDate", DateType, nullable = true),
    StructField("lastUpdateDate", DateType, nullable = false),
    StructField("entryDate", DateType, nullable = false),
    StructField("addressbasePostal", StringType, nullable = false),
    StructField("postcodeLocator", StringType, nullable = false),
    StructField("multiOccCount", ShortType, nullable = false)
  ))

  /**
    * Classification CSV file schema
    */
  val classificationFileSchema = StructType(Seq(
    StructField("recordIdentifier", ByteType, nullable = false),
    StructField("changeType", StringType, nullable = false),
    StructField("proOrder", LongType, nullable = false),
    StructField("uprn", LongType, nullable = false),
    StructField("classKey", StringType, nullable = false),
    StructField("classificationCode", StringType, nullable = false),
    StructField("classScheme", StringType, nullable = false),
    StructField("schemeVersion", FloatType, nullable = false),
    StructField("startDate", DateType, nullable = false),
    StructField("endDate", DateType, nullable = true),
    StructField("lastUpdateDate", DateType, nullable = false),
    StructField("entryDate", DateType, nullable = false)
  ))

  /**
    * crossref CSV file schema
    */
  val crossrefFileSchema = StructType(Seq(
    StructField("recordIdentifier", ByteType, nullable = false),
    StructField("changeType", StringType, nullable = false),
    StructField("proOrder", LongType, nullable = false),
    StructField("uprn", LongType, nullable = false),
    StructField("xrefKey", StringType, nullable = false),
    StructField("crossReference", StringType, nullable = false),
    StructField("version", IntegerType, nullable = true),
    StructField("source", StringType, nullable = false),
    StructField("startDate", DateType, nullable = false),
    StructField("endDate", DateType, nullable = true),
    StructField("lastUpdateDate", DateType, nullable = false),
    StructField("entryDate", DateType, nullable = false)
  ))

  /**
    * lpi CSV file schema
    */
  val lpiFileSchema = StructType(Seq(
    StructField("recordIdentifier", ByteType, nullable = false),
    StructField("changeType", StringType, nullable = false),
    StructField("proOrder", LongType, nullable = false),
    StructField("uprn", LongType, nullable = false),
    StructField("lpiKey", StringType, nullable = false),
    StructField("language", StringType, nullable = false),
    StructField("logicalStatus", ByteType, nullable = false),
    StructField("startDate", DateType, nullable = false),
    StructField("endDate", DateType, nullable = true),
    StructField("lastUpdateDate", DateType, nullable = false),
    StructField("entryDate", DateType, nullable = false),
    StructField("saoStartNumber", ShortType, nullable = true),
    StructField("saoStartSuffix", StringType, nullable = true),
    StructField("saoEndNumber", ShortType, nullable = true),
    StructField("saoEndSuffix", StringType, nullable = true),
    StructField("saoText", StringType, nullable = true),
    StructField("paoStartNumber", ShortType, nullable = true),
    StructField("paoStartSuffix", StringType, nullable = true),
    StructField("paoEndNumber", ShortType, nullable = true),
    StructField("paoEndSuffix", StringType, nullable = true),
    StructField("paoText", StringType, nullable = true),
    StructField("usrn", IntegerType, nullable = false),
    StructField("usrnMatchIndicator", ByteType, nullable = false),
    StructField("areaName", StringType, nullable = true),
    StructField("level", StringType, nullable = true),
    StructField("officialFlag", StringType, nullable = true)
  ))

  /**
    * organisation CSV file schema
    */
  val organisationFileSchema = StructType(Seq(
    StructField("recordIdentifier", ByteType, nullable = false),
    StructField("changeType", StringType, nullable = false),
    StructField("proOrder", LongType, nullable = false),
    StructField("uprn", LongType, nullable = false),
    StructField("orgKey", StringType, nullable = false),
    StructField("organisation", StringType, nullable = false),
    StructField("legalName", StringType, nullable = true),
    StructField("startDate", DateType, nullable = false),
    StructField("endDate", DateType, nullable = true),
    StructField("lastUpdateDate", DateType, nullable = false),
    StructField("entryDate", DateType, nullable = false)
  ))

  /**
    * street CSV file schema
    */
  val streetFileSchema = StructType(Seq(
    StructField("recordIdentifier", ByteType, nullable = false),
    StructField("changeType", StringType, nullable = false),
    StructField("proOrder", LongType, nullable = false),
    StructField("usrn", IntegerType, nullable = false),
    StructField("recordType", ByteType, nullable = false),
    StructField("swaOrgRefNaming", ShortType, nullable = false),
    StructField("state", ByteType, nullable = true),
    StructField("stateDate", DateType, nullable = true),
    StructField("streetSurface", ByteType, nullable = true),
    StructField("streetClassification", ByteType, nullable = true),
    StructField("version", ShortType, nullable = false),
    StructField("streetStartDate", DateType, nullable = false),
    StructField("streetEndDate", DateType, nullable = true),
    StructField("lastUpdateDate", DateType, nullable = false),
    StructField("recordEntryDate", DateType, nullable = false),
    StructField("streetStartX", FloatType, nullable = false),
    StructField("streetStartY", FloatType, nullable = false),
    StructField("streetStartLat", FloatType, nullable = false),
    StructField("streetStartLong", FloatType, nullable = false),
    StructField("streetEndX", FloatType, nullable = false),
    StructField("streetEndY", FloatType, nullable = false),
    StructField("streetEndLat", FloatType, nullable = false),
    StructField("streetEndLong", FloatType, nullable = false),
    StructField("streetTolerance", ShortType, nullable = false)
  ))

  /**
    * street-descriptor CSV file schema
    */
  val streetDescriptorFileSchema = StructType(Seq(
    StructField("recordIdentifier", ByteType, nullable = false),
    StructField("changeType", StringType, nullable = false),
    StructField("proOrder", LongType, nullable = false),
    StructField("usrn", IntegerType, nullable = false),
    StructField("streetDescriptor", StringType, nullable = false),
    StructField("locality", StringType, nullable = true),
    StructField("townName", StringType, nullable = true),
    StructField("administrativeArea", StringType, nullable = false),
    StructField("language", StringType, nullable = false),
    StructField("startDate", DateType, nullable = false),
    StructField("endDate", DateType, nullable = true),
    StructField("lastUpdateDate", DateType, nullable = false),
    StructField("entryDate", DateType, nullable = false)
  ))

  /**
    * successor CSV file schema
    */
  val successorFileSchema = StructType(Seq(
    StructField("recordIdentifier", ByteType, nullable = false),
    StructField("changeType", StringType, nullable = false),
    StructField("proOrder", LongType, nullable = false),
    StructField("uprn", LongType, nullable = false),
    StructField("succKey", StringType, nullable = false),
    StructField("startDate", DateType, nullable = false),
    StructField("endDate", DateType, nullable = true),
    StructField("lastUpdateDate", DateType, nullable = false),
    StructField("entryDate", DateType, nullable = false),
    StructField("successor", LongType, nullable = false)
  ))

  /**
    * hierarchy CSV file schema
    */
  val hierarchyFileSchema = StructType(Seq(
    StructField("uprn", LongType, nullable = false),
    StructField("primaryUprn", LongType, nullable = false),
    StructField("secondaryUprn", LongType, nullable = true),
    StructField("layers", IntegerType, nullable = false),
    StructField("thisLayer", IntegerType, nullable = false),
    StructField("parentUprn", LongType, nullable = true)
  ))
}