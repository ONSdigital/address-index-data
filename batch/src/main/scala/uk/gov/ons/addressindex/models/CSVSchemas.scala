package uk.gov.ons.addressindex.models

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object CSVSchemas {

  private val deliveryPointFields = List("RECORD_IDENTIFIER","CHANGE_TYPE","PRO_ORDER","UPRN","UDPRN",
    "ORGANISATION_NAME","DEPARTMENT_NAME","SUB_BUILDING_NAME","BUILDING_NAME","BUILDING_NUMBER",
    "DEPENDENT_THOROUGHFARE","THROUGHFARE","DOUBLE_DEPENDENT_LOCALITY","DEPENDENT_LOCALITY",
    "POST_TOWN","POSTCODE","POSTCODE_TYPE","DELIVERY_POINT_SUFFIX","WELSH_DEPENDENT_THOROUGHFARE",
    "WELSH_THOROUGHFARE","WELSH_DOUBLE_DEPENDENT_LOCALITY","WELSH_DEPENDENT_LOCALITY","WELSH_POST_TOWN",
    "PO_BOX_NUMBER","PROCESS_DATE","START_DATE","END_DATE","LAST_UPDATE_DATE","ENTRY_DATE")

  val deliveryPointSchema = StructType(
    deliveryPointFields.map(fieldName => StructField(fieldName, StringType, nullable = false))
  )


}
