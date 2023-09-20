package uk.gov.ons.addressindex.readers

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class AddressIndexFileReaderSpec extends AnyWordSpec with Matchers {

  val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
  /*
    The following line stops the error that is reported by travis when running the tests.
    This is a Derby DB issue which we don't use explicitly but Spark does for local test runs.
    https://issues.apache.org/jira/browse/SPARK-22918
   */
  System.setSecurityManager(null)

  "AddressIndexFileReader" should {
    "read delivery point csv file" in {

      // When
      val result = AddressIndexFileReader.readDeliveryPointCSV().collect()

      // Then
      result.length shouldBe 3 // 4 with the header

      val firstLine = result(0)

      firstLine.getByte(0) shouldBe 28 // RECORD_IDENTIFIER
      firstLine.getString(1) shouldBe "I" // CHANGE_TYPE
      firstLine.getLong(2) shouldBe 272650L // PRO_ORDER
      firstLine.getLong(3) shouldBe 1L // UPRN
      firstLine.getInt(4) shouldBe 19 // UDPRN
      firstLine.getString(5) shouldBe "CIBO" // ORGANISATION_NAME
      firstLine.getString(6) shouldBe "department" // DEPARTMENT_NAME
      firstLine.getString(7) shouldBe "FLAT E" // SUB_BUILDING_NAME
      firstLine.getString(8) shouldBe "COTTAGE" // BUILDING_NAME
      firstLine.getShort(9) shouldBe 1 // BUILDING_NUMBER
      firstLine.getString(10) shouldBe "throughfare" // DEPENDENT_THOROUGHFARE
      firstLine.getString(11) shouldBe "SOME_STREET" // THOUGHFARE (yes, typo in CSV header)
      firstLine.getString(12) shouldBe "locality" // DOUBLE_DEPENDENT_LOCALITY
      firstLine.getString(13) shouldBe "STIXTON" // DEPENDENT_LOCALITY
      firstLine.getString(14) shouldBe "LONDON" // POST_TOWN
      firstLine.getString(15) shouldBe "POSTCODE" // POSTCODE
      firstLine.getString(16) shouldBe "S" // POSTCODE_TYPE
      firstLine.getString(17) shouldBe "1Q" // DELIVERY_POINT_SUFFIX
      firstLine.getString(18) shouldBe "welsh1" // WELSH_DEPENDENT_THOROUGHFARE
      firstLine.getString(19) shouldBe "welsh2" // WELSH_THOROUGHFARE
      firstLine.getString(20) shouldBe "welsh3" // WELSH_DOUBLE_DEPENDENT_LOCALITY
      firstLine.getString(21) shouldBe "welsh4" // WELSH_DEPENDENT_LOCALITY
      firstLine.getString(22) shouldBe "welsh5" // WELSH_POST_TOWN
      firstLine.getString(23) shouldBe "6" // PO_BOX_NUMBER
      firstLine.getDate(24) shouldBe format.parse("2016-01-18") // PROCESS_DATE
      firstLine.getDate(25) shouldBe format.parse("2012-04-23") // START_DATE
      firstLine.getDate(26) shouldBe format.parse("2012-04-25") // END_DATE
      firstLine.getDate(27) shouldBe format.parse("2016-02-10") // LAST_UPDATE_DATE
      firstLine.getDate(28) shouldBe format.parse("2012-03-19") // ENTRY_DATE
    }

    "read blpu csv file" in {

      // When
      val result = AddressIndexFileReader.readBlpuCSV().collect()

      // Then
      result.length shouldBe 4 // 5 with the header

      val firstLine = result(0)

      firstLine.getByte(0) shouldBe 21 // RECORD_IDENTIFIER
      firstLine.getString(1) shouldBe "I" // CHANGE_TYPE
      firstLine.getLong(2) shouldBe 373713L // PRO_ORDER
      firstLine.getLong(3) shouldBe 2L // UPRN
      firstLine.getByte(4) shouldBe 1 // LOGICAL_STATUS
      firstLine.getByte(5) shouldBe 3 // BLPU_STATE
      firstLine.getDate(6) shouldBe format.parse("2007-06-26") // BLPU_STATE_DATE
      firstLine.getLong(7) shouldBe 999910971564L // PARENT_UPRN
      firstLine.getFloat(8) shouldBe 379203.00F // X_COORDINATE
      firstLine.getFloat(9) shouldBe 412780.00F // Y_COORDINATE
      firstLine.getFloat(10) shouldBe 53.6111710F // LATITUDE
      firstLine.getFloat(11) shouldBe -2.3158117F // LONGITUDE
      firstLine.getByte(12) shouldBe 1 // RPC
      firstLine.getShort(13) shouldBe 4218 // LOCAL_CUSTODIAN_CODE
      firstLine.getString(14) shouldBe "E" // COUNTRY
      firstLine.getDate(15) shouldBe format.parse("2007-06-26") // START_DATE
      firstLine.getDate(16) shouldBe format.parse("2099-02-10") // END_DATE
      firstLine.getDate(17) shouldBe format.parse("2016-02-10") // LAST_UPDATE_DATE
      firstLine.getDate(18) shouldBe format.parse("2003-09-18") // ENTRY_DATE
      firstLine.getString(19) shouldBe "D" // ADDRESSBASE_POSTAL
      firstLine.getString(20) shouldBe "KL8 1JQ" // POSTCODE_LOCATOR
      firstLine.getShort(21) shouldBe 0 // MULTI_OCC_COUNT
    }

    "read classification csv file" in {

      // When
      val result = AddressIndexFileReader.readClassificationCSV().collect()

      // Then
      result.length shouldBe 4 // 5 with the header

      val firstLine = result(0)

      firstLine.getByte(0) shouldBe 32 // RECORD_IDENTIFIER
      firstLine.getString(1) shouldBe "I" // CHANGE_TYPE
      firstLine.getLong(2) shouldBe 683979L // PRO_ORDER
      firstLine.getLong(3) shouldBe 2L // UPRN
      firstLine.getString(4) shouldBe "1110C000053211" // CLASS_KEY
      firstLine.getString(5) shouldBe "RD" // CLASSIFICATION_CODE
      firstLine.getString(6) shouldBe "AddressBase Premium Classification Scheme" // CLASS_SCHEME
      firstLine.getFloat(7) shouldBe 2.00F // SCHEME_VERSION
      firstLine.getDate(8) shouldBe format.parse("2017-10-24") // START_DATE
      firstLine.getDate(9) shouldBe format.parse("2019-10-28") // END_DATE
      firstLine.getDate(10) shouldBe format.parse("2015-03-10") // LAST_UPDATE_DATE
      firstLine.getDate(11) shouldBe format.parse("2001-05-05") // ENTRY_DATE
    }

    "read crossref csv file" in {

      // When
      val result = AddressIndexFileReader.readCrossrefCSV().collect()

      // Then
      result.length shouldBe 10 // 9 with the header

      val firstLine = result(0)

      firstLine.getByte(0) shouldBe 23 // RECORD_IDENTIFIER
      firstLine.getString(1) shouldBe "I" // CHANGE_TYPE
      firstLine.getLong(2) shouldBe 211153L // PRO_ORDER
      firstLine.getLong(3) shouldBe 2L // UPRN
      firstLine.getString(4) shouldBe "0655X202358935" // XREF_KEY
      firstLine.getString(5) shouldBe "E04000324" // CROSS_REFERENCE
      firstLine.getInt(6) shouldBe 123 // VERSION
      firstLine.getString(7) shouldBe "7666MI" // SOURCE
      firstLine.getDate(8) shouldBe format.parse("2015-02-07") // START_DATE
      firstLine.getDate(9) shouldBe format.parse("2017-01-01") // END_DATE
      firstLine.getDate(10) shouldBe format.parse("2012-02-09") // LAST_UPDATE_DATE
      firstLine.getDate(11) shouldBe format.parse("2015-05-07") // ENTRY_DATE
    }

    "read lpi csv file" in {

      // When
      val result = AddressIndexFileReader.readLpiCSV().collect()

      // Then
      result.length shouldBe 9 // 10 with the header

      val firstLine = result(0)

      firstLine.getByte(0) shouldBe 24 // RECORD_IDENTIFIER
      firstLine.getString(1) shouldBe "I" // CHANGE_TYPE
      firstLine.getLong(2) shouldBe 309891L // PRO_ORDER
      firstLine.getLong(3) shouldBe 2L // UPRN
      firstLine.getString(4) shouldBe "1610L000015314" // LPI_KEY
      firstLine.getString(5) shouldBe "ENG" // LANGUAGE
      firstLine.getByte(6) shouldBe 1 // LOGICAL_STATUS
      firstLine.getDate(7) shouldBe format.parse("2007-10-10") // START_DATE
      firstLine.getDate(8) shouldBe format.parse("2018-01-11") // END_DATE
      firstLine.getDate(9) shouldBe format.parse("2016-03-11") // LAST_UPDATE_DATE
      firstLine.getDate(10) shouldBe format.parse("2007-06-30") // ENTRY_DATE
      firstLine.getShort(11) shouldBe 1234 // SAO_START_NUMBER
      firstLine.getString(12) shouldBe "AA" // SAO_START_SUFFIX
      firstLine.getShort(13) shouldBe 5678 // SAO_END_NUMBER
      firstLine.getString(14) shouldBe "BB" // SAO_END_SUFFIX
      firstLine.getString(15) shouldBe "A BUILDING NAME OR DESCRIPTION" // SAO_TEXT
      firstLine.getShort(16) shouldBe 15 // PAO_START_NUMBER
      firstLine.getString(17) shouldBe "CC" // PAO_START_SUFFIX
      firstLine.getShort(18) shouldBe 9876 // PAO_END_NUMBER
      firstLine.getString(19) shouldBe "AB" // PAO_END_SUFFIX
      firstLine.getString(20) shouldBe "ANOTHER BUILDING NAME OR DESCRIPTION" // PAO_TEXT
      firstLine.getInt(21) shouldBe 9401385 // USRN
      firstLine.getByte(22) shouldBe 1 // USRN_MATCH_INDICATOR
      firstLine.getString(23) shouldBe "GEOGRAPHIC AREA NAME" // AREA_NAME
      firstLine.getString(24) shouldBe "VERTICAL POSITION" // LEVEL
      firstLine.getString(25) shouldBe "Y" // OFFICIAL_FLAG
    }

    "read organisation csv file" in {

      // When
      val result = AddressIndexFileReader.readOrganisationCSV().collect()

      // Then
      result.length shouldBe 3 // 4 with the header

      val firstLine = result(0)

      firstLine.getByte(0) shouldBe 31 // RECORD_IDENTIFIER
      firstLine.getString(1) shouldBe "I" // CHANGE_TYPE
      firstLine.getLong(2) shouldBe 357282L // PRO_ORDER
      firstLine.getLong(3) shouldBe 2L // UPRN
      firstLine.getString(4) shouldBe "0116O000707611" // ORG_KEY
      firstLine.getString(5) shouldBe "SOME COUNCIL" // ORGANISATION
      firstLine.getString(6) shouldBe "THE LEGAL NAME" // LEGAL_NAME
      firstLine.getDate(7) shouldBe format.parse("2013-09-24") // START_DATE
      firstLine.getDate(8) shouldBe format.parse("2019-03-11") // END_DATE
      firstLine.getDate(9) shouldBe format.parse("2017-02-10") // LAST_UPDATE_DATE
      firstLine.getDate(10) shouldBe format.parse("1997-01-16") // ENTRY_DATE
    }

    "read street csv file" in {

      // When
      val result = AddressIndexFileReader.readStreetCSV().collect()

      // Then
      result.length shouldBe 3 // 4 with the header

      val firstLine = result(0)

      firstLine.getByte(0) shouldBe 11 // RECORD_IDENTIFIER
      firstLine.getString(1) shouldBe "I" // CHANGE_TYPE
      firstLine.getLong(2) shouldBe 1L // PRO_ORDER
      firstLine.getInt(3) shouldBe 9401385 // USRN
      firstLine.getByte(4) shouldBe 1 // RECORD_TYPE
      firstLine.getShort(5) shouldBe 1350 // SWA_ORG_REF_NAMING
      firstLine.getByte(6) shouldBe 2 // STATE
      firstLine.getDate(7) shouldBe format.parse("2001-10-07") // STATE_DATE
      firstLine.getByte(8) shouldBe 1 // STREET_SURFACE
      firstLine.getByte(9) shouldBe 8 // STREET_CLASSIFICATION
      firstLine.getShort(10) shouldBe 0 // VERSION
      firstLine.getDate(11) shouldBe format.parse("2017-10-17") // STREET_START_DATE
      firstLine.getDate(12) shouldBe format.parse("2099-11-11")// STREET_END_DATE
      firstLine.getDate(13) shouldBe format.parse("2015-04-09") // LAST_UPDATE_DATE
      firstLine.getDate(14) shouldBe format.parse("2014-02-09") // RECORD_ENTRY_DATE
      firstLine.getFloat(15) shouldBe 426744.00F // STREET_START_X
      firstLine.getFloat(16) shouldBe 511293.00F // STREET_START_Y
      firstLine.getFloat(17) shouldBe 54.5399359F // STREET_START_LAT
      firstLine.getFloat(18) shouldBe -1.5450196F // STREET_START_LONG
      firstLine.getFloat(19) shouldBe 458259.00F // STREET_END_X
      firstLine.getFloat(20) shouldBe 517754.00F // STREET_END_Y
      firstLine.getFloat(21) shouldBe 54.5553859F // STREET_END_LAT
      firstLine.getFloat(22) shouldBe -1.5348010F // STREET_END_LONG
      firstLine.getShort(23) shouldBe 1 // STREET_TOLERANCE
    }

    "read street-descriptor csv file" in {

      // When
      val result = AddressIndexFileReader.readStreetDescriptorCSV().collect()

      // Then
      result.length shouldBe 3 // 4 with the header

      val firstLine = result(0)

      firstLine.getByte(0) shouldBe 15 // RECORD_IDENTIFIER
      firstLine.getString(1) shouldBe "I" // CHANGE_TYPE
      firstLine.getLong(2) shouldBe 334565L // PRO_ORDER
      firstLine.getInt(3) shouldBe 9401385 // USRN
      firstLine.getString(4) shouldBe "A STREET DESCRIPTOR" // STREET_DESCRIPTOR
      firstLine.getString(5) shouldBe "A GREAT LOCALITY" // LOCALITY
      firstLine.getString(6) shouldBe "TOWNY TOWN" // TOWN_NAME
      firstLine.getString(7) shouldBe "ADMIN AREA A" // ADMINSTRATIVE_AREA
      firstLine.getString(8) shouldBe "ENG" // LANGUAGE
      firstLine.getDate(9) shouldBe format.parse("2015-05-16") // START_DATE
      firstLine.getDate(10) shouldBe format.parse("2015-05-16") // END_DATE
      firstLine.getDate(11) shouldBe format.parse("2015-02-06") // LAST_UPDATE_DATE
      firstLine.getDate(12) shouldBe format.parse("2005-03-18") // ENTRY_DATE
    }

    "read successor csv file" in {

      // When
      val result = AddressIndexFileReader.readSuccessorCSV().collect()

      // Then
      result.length shouldBe 3 // 4 with the header

      val firstLine = result(0)

      firstLine.getByte(0) shouldBe 30 // RECORD_IDENTIFIER
      firstLine.getString(1) shouldBe "I" // CHANGE_TYPE
      firstLine.getLong(2) shouldBe 12345L // PRO_ORDER
      firstLine.getLong(3) shouldBe 100105677917L // UPRN
      firstLine.getString(4) shouldBe "9078S000000011" // SUCC_KEY
      firstLine.getDate(5) shouldBe format.parse("2006-11-11") // START_DATE
      firstLine.getDate(6) shouldBe format.parse("2027-11-15") // END_DATE
      firstLine.getDate(7) shouldBe format.parse("2007-11-15") // LAST_UPDATE_DATE
      firstLine.getDate(8) shouldBe format.parse("2006-10-10") // ENTRY_DATE
      firstLine.getLong(9) shouldBe 122000001L // SUCCESSOR
    }

    "read hierarchy csv file" in {

      // When
      val result = AddressIndexFileReader.readHierarchyCSV().collect()

      // Then
      result.length shouldBe 12

      val line = result(4)
      line.getLong(0) shouldBe 5 // UPRN
      line.getLong(1) shouldBe 1 // PRIMARY_UPRN
      line.getLong(2) shouldBe 2 // SECONDARY_UPRN
      line.getInt(3) shouldBe 3 // LAYERS
      line.getInt(4) shouldBe 3 // CURRENT_LAYER
      line.getLong(2) shouldBe 2 // PARENT_UPRN
    }

    "read rdmf csv file" in {

      // When
      val result = AddressIndexFileReader.readRDMFCSV().collect()

      // Then
      result.length shouldBe 4

      val line = result(3)

      line.getLong(0) shouldBe 99 // UPRN
      line.getLong(1) shouldBe 100000034563801L // ADDRESS_ENTRY_ID
      line.getLong(2) shouldBe 95 // EPOCH
      line.getString(3) shouldBe "A100000034563801" // ADDRESS_ENTRY_ID_ALPHANUMERIC_BACKUP

    }


    "extract epoch from the file path" ignore {
      // Given
      val filePath = "hdfs://path/to/file/ABP_E39_BLPU_v040506"
      val expected = 39

      // When
      val result = AddressIndexFileReader.extractEpoch(filePath)

      // Then
      result shouldBe expected
    }

    "throw exception if no epoch could be extracted" ignore {
      // Given
      val filePath = "hdfs://path/to/file/ABP_E_BLPU_v040506.csv"

      // When Then
      intercept[IllegalArgumentException]{
        AddressIndexFileReader.extractEpoch(filePath)
      }
    }

    "extract date from the file path" in {
      // Given
      val filePath = "gs://aims-ons-abp-raw-full-e102-1037392368223_backup/ai_aims_delivery_point_current_20230913.csv"
      val expected = "20230913"

      // When
      val result = AddressIndexFileReader.extractDate(filePath)

      // Then
      result shouldBe expected
    }

    "throw exception if no date could be extracted" in {
      // Given
      val filePath = "gs://aims-ons-abp-raw-full-e102-1037392368223_backup/ai_aims_delivery_point_current_star_wars.csv"

      // When Then
      intercept[IllegalArgumentException]{
        AddressIndexFileReader.extractDate(filePath)
      }
    }

    "return true if file could be validated" ignore {
      // Given
      val filePath = "hdfs://path/to/file/ABP_E39_BLPU_v040506.csv"
      val epoch = 39
      val date = "040506"

      // When
      val result = AddressIndexFileReader.validateFileName(filePath, epoch, date)

      // Then
      result shouldBe true
    }

    "throw exception if file could not be validated" ignore {
      // reinstate test when we have consistent dates in DAP files
      // Given
      val filePath = "hdfs://path/to/file/ABP_E39_BLPU.csv"
      val epoch = 40
      val date = "010203"

      // When Then
      intercept[IllegalArgumentException]{
        AddressIndexFileReader.validateFileName(filePath, epoch, date)
      }

    }
  }
}