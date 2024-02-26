package uk.gov.ons.addressindex.models

import org.apache.spark.sql.types._

/**
  * Contains schemas that should be applied to CSV documents
  */
object CSVSchemas {

  /**
    * Postcode address CSV file schema
    * RECORD_IDENTIFIER,CHANGE_TYPE,PRO_ORDER,UPRN,UDPRN,ORGANISATION_NAME,DEPARTMENT_NAME,SUB_BUILDING_NAME,BUILDING_NAME,BUILDING_NUMBER,DEPENDENT_THOROUGHFARE,THOROUGHFARE,DOUBLE_DEPENDENT_LOCALITY,DEPENDENT_LOCALITY,POST_TOWN,POSTCODE,POSTCODE_TYPE,DELIVERY_POINT_SUFFIX,WELSH_DEPENDENT_THOROUGHFARE,WELSH_THOROUGHFARE,WELSH_DOUBLE_DEPENDENT_LOCALITY,WELSH_DEPENDENT_LOCALITY,WELSH_POST_TOWN,PO_BOX_NUMBER,PROCESS_DATE,START_DATE,END_DATE,LAST_UPDATE_DATE,ENTRY_DATE,EPOCH
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
    * RECORD_IDENTIFIER,CHANGE_TYPE,PRO_ORDER,UPRN,LOGICAL_STATUS,BLPU_STATE,BLPU_STATE_DATE,PARENT_UPRN,X_COORDINATE,Y_COORDINATE,LATITUDE,LONGITUDE,RPC,LOCAL_CUSTODIAN_CODE,COUNTRY,START_DATE,END_DATE,LAST_UPDATE_DATE,ENTRY_DATE,ADDRESSBASE_POSTAL,POSTCODE_LOCATOR,MULTI_OCC_COUNT,EPOCH
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
    * RECORD_IDENTIFIER,CHANGE_TYPE,PRO_ORDER,UPRN,CLASS_KEY,CLASSIFICATION_CODE,CLASS_SCHEME,SCHEME_VERSION,START_DATE,END_DATE,LAST_UPDATE_DATE,ENTRY_DATE,EPOCH
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
    * RECORD_IDENTIFIER,CHANGE_TYPE,PRO_ORDER,UPRN,XREF_KEY,CROSS_REFERENCE,VERSION,SOURCE,START_DATE,END_DATE,LAST_UPDATE_DATE,ENTRY_DATE,EPOCH
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
    * RECORD_IDENTIFIER,CHANGE_TYPE,PRO_ORDER,UPRN,LPI_KEY,LANGUAGE,LOGICAL_STATUS,START_DATE,END_DATE,LAST_UPDATE_DATE,ENTRY_DATE,SAO_START_NUMBER,SAO_START_SUFFIX,SAO_END_NUMBER,SAO_END_SUFFIX,SAO_TEXT,PAO_START_NUMBER,PAO_START_SUFFIX,PAO_END_NUMBER,PAO_END_SUFFIX,PAO_TEXT,USRN,USRN_MATCH_INDICATOR,AREA_NAME,LEVEL,OFFICIAL_FLAG,EPOCH
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
    * RECORD_IDENTIFIER,CHANGE_TYPE,PRO_ORDER,UPRN,ORG_KEY,ORGANISATION,LEGAL_NAME,START_DATE,END_DATE,LAST_UPDATE_DATE,ENTRY_DATE,EPOCH
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
    * RECORD_IDENTIFIER,CHANGE_TYPE,PRO_ORDER,USRN,RECORD_TYPE,SWA_ORG_REF_NAMING,STATE,STATE_DATE,STREET_SURFACE,STREET_CLASSIFICATION,VERSION,STREET_START_DATE,STREET_END_DATE,LAST_UPDATE_DATE,RECORD_ENTRY_DATE,STREET_START_X,STREET_START_Y,STREET_START_LAT,STREET_START_LONG,STREET_END_X,STREET_END_Y,STREET_END_LAT,STREET_END_LONG,STREET_TOLERANCE,EPOCH
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
    * RECORD_IDENTIFIER,CHANGE_TYPE,PRO_ORDER,USRN,STREET_DESCRIPTOR,LOCALITY,TOWN_NAME,ADMINSTRATIVE_AREA,LANGUAGE,START_DATE,END_DATE,LAST_UPDATE_DATE,ENTRY_DATE,EPOCH
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
    * UPRN,PRIMARY_UPRN,SECONDARY_UPRN,LAYERS,THIS_LAYER,PARENT_UPRN,ADDRESS_TYPE,ESTAB_TYPE,EPOCH
    */
  val hierarchyFileSchema = StructType(Seq(
        StructField("uprn", LongType, nullable = false),
        StructField("primaryUprn", LongType, nullable = false),
        StructField("secondaryUprn", LongType, nullable = true),
        StructField("layers", IntegerType, nullable = false),
        StructField("thisLayer", IntegerType, nullable = false),
        StructField("parentUprn", LongType, nullable = true),
        StructField("addressType", StringType, nullable = true),
        StructField("estabType", StringType, nullable = true)
  ))

  /**
    * RDMF initial test CSV file schema
    */
  val rdmfTestFileSchema = StructType(Seq(
    StructField("address_entry_id", LongType, nullable = false),
    StructField("detail_valid_from_date", DateType, nullable = false),
    StructField("standard_address_source_code", StringType, nullable = true),
    StructField("language", StringType, nullable = true),
    StructField("detail_valid_to_date", DateType, nullable = true),
    StructField("uprn", LongType, nullable = false),
    StructField("udprn", LongType, nullable = true),
    StructField("address_line_1", StringType, nullable = true),
    StructField("address_line_2", StringType, nullable = true),
    StructField("address_line_3", StringType, nullable = true),
    StructField("address_line_4", StringType, nullable = true),
    StructField("address_line_5", StringType, nullable = true),
    StructField("town", StringType, nullable = true),
    StructField("postcode", StringType, nullable = true),
    StructField("country", StringType, nullable = true),
    StructField("logical_status", IntegerType, nullable = false),
    StructField("blpu_state", IntegerType, nullable = false),
    StructField("addressbase_postal", StringType, nullable = true),
    StructField("easting", FloatType, nullable = false),
    StructField("northing", FloatType, nullable = false),
    StructField("latitude", FloatType, nullable = false),
    StructField("longitude", FloatType, nullable = false),
    StructField("establishment_type_id", StringType, nullable = true),
    StructField("class_scheme", StringType, nullable = true),
    StructField("scheme_version", StringType, nullable = true),
    StructField("classification_code", LongType, nullable = true),
    StructField("classification_entry_date", DateType, nullable = false),
    StructField("classification_end_date", DateType, nullable = true),
    StructField("entry_date", DateType, nullable = false),
    StructField("end_date", DateType, nullable = true),
    StructField("parent_uprn", LongType, nullable = true),
    StructField("primary_uprn", LongType, nullable = false),
    StructField("secondary_uprn", LongType, nullable = true),
    StructField("ons_oa_id", LongType, nullable = true),
    StructField("la_code", StringType, nullable = true),
    StructField("oa_code", StringType, nullable = true),
    StructField("lsoa_code", StringType, nullable = true),
    StructField("msoa_code", StringType, nullable = true)
  ))

  /**
    * RDMF address_entry_id to uprn lookup file schema
    */
  val rdmfFileSchema = StructType(Seq(
    StructField("uprn", LongType, nullable = false),
    StructField("address_entry_id", LongType, nullable = false),
    StructField("epoch", LongType, nullable = false),
    StructField("address_entry_id_alphanumeric_backup", StringType, nullable = false),
  ))

}