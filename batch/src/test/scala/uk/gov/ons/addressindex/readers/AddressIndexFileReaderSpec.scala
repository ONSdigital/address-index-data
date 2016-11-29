package uk.gov.ons.addressindex.readers

import org.scalatest.{Matchers, WordSpec}

class AddressIndexFileReaderSpec extends WordSpec with Matchers {
  "AddressIndexFileReader" should {
    "read delivery point csv file" in {

      // When
      val result = AddressIndexFileReader.readDeliveryPointCSV().collect()

      // Then
      result.length shouldBe 3 // 4 with the header

      val firstLine = result(0)

      firstLine.getString(0) shouldBe "28" // RECORD_IDENTIFIER
      firstLine.getString(1) shouldBe "I" // CHANGE_TYPE
      firstLine.getString(2) shouldBe "272650" // PRO_ORDER
      firstLine.getString(3) shouldBe "1" // UPDN
      firstLine.getString(4) shouldBe "19" // UDPRN
      firstLine.getString(5) shouldBe "CIBO" // ORGANISATION_NAME
      firstLine.getString(6) shouldBe "department" // DEPARTMENT_NAME
      firstLine.getString(7) shouldBe "FLAT E" // SUB_BUILDING_NAME
      firstLine.getString(8) shouldBe "COTTAGE" // BUILDING_NAME
      firstLine.getString(9) shouldBe "1" // BUILDING_NUMBER
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
      firstLine.getString(20) shouldBe "welsh3" // WELSH_DOUBLE_DEPENDENT_LOCALITY"
      firstLine.getString(21) shouldBe "welsh4" // WELSH_DEPENDENT_LOCALITY
      firstLine.getString(22) shouldBe "welsh5" // WELSH_POST_TOWN
      firstLine.getString(23) shouldBe "6" // PO_BOX_NUMBER
      firstLine.getString(24) shouldBe "2016-01-18" // PROCESS_DATE
      firstLine.getString(25) shouldBe "2012-04-23" // START_DATE
      firstLine.getString(26) shouldBe "2012-04-25" // END_DATE
      firstLine.getString(27) shouldBe "2016-02-10" // LAST_UPDATE_DATE
      firstLine.getString(28) shouldBe "2012-03-19" // ENTRY_DATE

    }

    "read blpu csv file" in {

      // When
      val result = AddressIndexFileReader.readBlpuCSV().collect()

      // Then
      result.length shouldBe 3 // 4 with the header

      val firstLine = result(0)

      firstLine.getString(0) shouldBe "21" // RECORD_IDENTIFIER
      firstLine.getString(1) shouldBe "I" // CHANGE_TYPE
      firstLine.getString(2) shouldBe "373713" // PRO_ORDER
      firstLine.getString(3) shouldBe "100010971564" // UPRN
      firstLine.getString(4) shouldBe "1" // LOGICAL_STATUS
      firstLine.getString(5) shouldBe "3" // BLPU_STATE
      firstLine.getString(6) shouldBe "2007-06-26" // BLPU_STATE_DATE
      firstLine.getString(7) shouldBe "999910971564" // PARENT_UPRN
      firstLine.getString(8) shouldBe "379203.00" // X_COORDINATE
      firstLine.getString(9) shouldBe "412780.00" // Y_COORDINATE
      firstLine.getString(10) shouldBe "53.6111710" // LATITUDE
      firstLine.getString(11) shouldBe "-2.3158117" // LONGITUDE
      firstLine.getString(12) shouldBe "1" // RPC
      firstLine.getString(13) shouldBe "4218" // LOCAL_CUSTODIAN_CODE
      firstLine.getString(14) shouldBe "E" // COUNTRY
      firstLine.getString(15) shouldBe "2007-06-26" // START_DATE
      firstLine.getString(16) shouldBe "2099-02-10" // END_DATE
      firstLine.getString(17) shouldBe "2016-02-10" // LAST_UPDATE_DATE
      firstLine.getString(18) shouldBe "2003-09-18" // ENTRY_DATE
      firstLine.getString(19) shouldBe "D" // ADDRESSBASE_POSTAL
      firstLine.getString(20) shouldBe "KL8 1JQ" // POSTCODE_LOCATOR
      firstLine.getString(21) shouldBe "0" // MULTI_OCC_COUNT
    }

    "read classification csv file" in {

      // When
      val result = AddressIndexFileReader.readClassificationCSV().collect()

      // Then
      result.length shouldBe 3 // 4 with the header

      val firstLine = result(0)

      firstLine.getString(0) shouldBe "32" // RECORD_IDENTIFIER
      firstLine.getString(1) shouldBe "I" // CHANGE_TYPE
      firstLine.getString(2) shouldBe "683979" // PRO_ORDER
      firstLine.getString(3) shouldBe "100040241256" // UPRN
      firstLine.getString(4) shouldBe "1110C000053211" // CLASS_KEY
      firstLine.getString(5) shouldBe "RD" // CLASSIFICATION_CODE
      firstLine.getString(6) shouldBe "AddressBase Premium Classification Scheme" // CLASS_SCHEME
      firstLine.getString(7) shouldBe "2.00" // SCHEME_VERSION
      firstLine.getString(8) shouldBe "2017-10-24" // START_DATE
      firstLine.getString(9) shouldBe "2019-10-28" // END_DATE
      firstLine.getString(10) shouldBe "2015-03-10" // LAST_UPDATE_DATE
      firstLine.getString(11) shouldBe "2001-05-05" // ENTRY_DATE
    }

    "read crossref csv file" in {

      // When
      val result = AddressIndexFileReader.readCrossrefCSV().collect()

      // Then
      result.length shouldBe 3 // 4 with the header

      val firstLine = result(0)

      firstLine.getString(0) shouldBe "23" // RECORD_IDENTIFIER
      firstLine.getString(1) shouldBe "I" // CHANGE_TYPE
      firstLine.getString(2) shouldBe "211153" // PRO_ORDER
      firstLine.getString(3) shouldBe "10090373276" // UPRN
      firstLine.getString(4) shouldBe "0655X202358935" // XREF_KEY
      firstLine.getString(5) shouldBe "E04000324" // CROSS_REFERENCE
      firstLine.getString(6) shouldBe "123" // VERSION
      firstLine.getString(7) shouldBe "7666MI" // SOURCE
      firstLine.getString(8) shouldBe "2015-02-07" // START_DATE
      firstLine.getString(9) shouldBe "2017-01-01" // END_DATE
      firstLine.getString(10) shouldBe "2012-02-09" // LAST_UPDATE_DATE
      firstLine.getString(11) shouldBe "2015-05-07" // ENTRY_DATE
    }

    "read lpi csv file" in {

      // When
      val result = AddressIndexFileReader.readLpiCSV().collect()

      // Then
      result.length shouldBe 3 // 4 with the header

      val firstLine = result(0)

      firstLine.getString(0) shouldBe "24" // RECORD_IDENTIFIER
      firstLine.getString(1) shouldBe "I" // CHANGE_TYPE
      firstLine.getString(2) shouldBe "309891" // PRO_ORDER
      firstLine.getString(3) shouldBe "10013693666" // UPRN
      firstLine.getString(4) shouldBe "1610L000015314" // LPI_KEY
      firstLine.getString(5) shouldBe "ENG" // LANGUAGE
      firstLine.getString(6) shouldBe "1" // LOGICAL_STATUS
      firstLine.getString(7) shouldBe "2007-10-10" // START_DATE
      firstLine.getString(8) shouldBe "2018-01-11" // END_DATE
      firstLine.getString(9) shouldBe "2016-03-11" // LAST_UPDATE_DATE
      firstLine.getString(10) shouldBe "2007-06-30" // ENTRY_DATE
      firstLine.getString(11) shouldBe "1234" // SAO_START_NUMBER
      firstLine.getString(12) shouldBe "AA" // SAO_START_SUFFIX
      firstLine.getString(13) shouldBe "5678" // SAO_END_NUMBER
      firstLine.getString(14) shouldBe "BB" // SAO_END_SUFFIX
      firstLine.getString(15) shouldBe "A BUILDING NAME OR DESCRIPTION" // SAO_TEXT
      firstLine.getString(16) shouldBe "15" // PAO_START_NUMBER
      firstLine.getString(17) shouldBe "CC" // PAO_START_SUFFIX
      firstLine.getString(18) shouldBe "9876" // PAO_END_NUMBER
      firstLine.getString(19) shouldBe "AB" // PAO_END_SUFFIX
      firstLine.getString(20) shouldBe "ANOTHER BUILDING NAME OR DESCRIPTION" // PAO_TEXT
      firstLine.getString(21) shouldBe "9401385" // USRN
      firstLine.getString(22) shouldBe "1" // USRN_MATCH_INDICATOR
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

      firstLine.getString(0) shouldBe "31" // RECORD_IDENTIFIER
      firstLine.getString(1) shouldBe "I" // CHANGE_TYPE
      firstLine.getString(2) shouldBe "357282" // PRO_ORDER
      firstLine.getString(3) shouldBe "1" // UPRN
      firstLine.getString(4) shouldBe "0116O000707611" // ORG_KEY
      firstLine.getString(5) shouldBe "SOME COUNCIL" // ORGANISATION
      firstLine.getString(6) shouldBe "THE LEGAL NAME" // LEGAL_NAME
      firstLine.getString(7) shouldBe "2013-09-24" // START_DATE
      firstLine.getString(8) shouldBe "2019-03-11" // END_DATE
      firstLine.getString(9) shouldBe "2017-02-10" // LAST_UPDATE_DATE
      firstLine.getString(10) shouldBe "1997-01-16" // ENTRY_DATE
    }

    "read street csv file" in {

      // When
      val result = AddressIndexFileReader.readStreetCSV().collect()

      // Then
      result.length shouldBe 3 // 4 with the header

      val firstLine = result(0)

      firstLine.getString(0) shouldBe "11" // RECORD_IDENTIFIER
      firstLine.getString(1) shouldBe "I" // CHANGE_TYPE
      firstLine.getString(2) shouldBe "1" // PRO_ORDER
      firstLine.getString(3) shouldBe "10416245" // USRN
      firstLine.getString(4) shouldBe "1" // RECORD_TYPE
      firstLine.getString(5) shouldBe "1350" // SWA_ORG_REF_NAMING
      firstLine.getString(6) shouldBe "2" // STATE
      firstLine.getString(7) shouldBe "2001-10-07" // STATE_DATE
      firstLine.getString(8) shouldBe "1" // STREET_SURFACE
      firstLine.getString(9) shouldBe "8" // STREET_CLASSIFICATION
      firstLine.getString(10) shouldBe "0" // VERSION
      firstLine.getString(11) shouldBe "2017-10-17" // STREET_START_DATE
      firstLine.getString(12) shouldBe "2099-11-11" // STREET_END_DATE
      firstLine.getString(13) shouldBe "2015-04-09" // LAST_UPDATE_DATE
      firstLine.getString(14) shouldBe "2014-02-09" // RECORD_ENTRY_DATE
      firstLine.getString(15) shouldBe "426744.00" // STREET_START_X
      firstLine.getString(16) shouldBe "511293.00" // STREET_START_Y
      firstLine.getString(17) shouldBe "54.5399359" // STREET_START_LAT
      firstLine.getString(18) shouldBe "-1.5450196" // STREET_START_LONG
      firstLine.getString(19) shouldBe "458259.00" // STREET_END_X
      firstLine.getString(20) shouldBe "517754.00" // STREET_END_Y
      firstLine.getString(21) shouldBe "54.5553859" // STREET_END_LAT
      firstLine.getString(22) shouldBe "-1.5348010" // STREET_END_LONG
      firstLine.getString(23) shouldBe "1" // STREET_TOLERANCE
    }

    "read street-descriptor csv file" in {

      // When
      val result = AddressIndexFileReader.readStreetDescriptorCSV().collect()

      // Then
      result.length shouldBe 3 // 4 with the header

      val firstLine = result(0)

      firstLine.getString(0) shouldBe "15" // RECORD_IDENTIFIER
      firstLine.getString(1) shouldBe "I" // CHANGE_TYPE
      firstLine.getString(2) shouldBe "334565" // PRO_ORDER
      firstLine.getString(3) shouldBe "84206901" // USRN
      firstLine.getString(4) shouldBe "A STREET DESCRIPTOR" // STREET_DESCRIPTOR
      firstLine.getString(5) shouldBe "LOCALITY" // LOCALITY
      firstLine.getString(6) shouldBe "TOWN" // TOWN_NAME
      firstLine.getString(7) shouldBe "ADMIN AREA A" // ADMINSTRATIVE_AREA
      firstLine.getString(8) shouldBe "ENG" // LANGUAGE
      firstLine.getString(9) shouldBe "2015-05-16" // START_DATE
      firstLine.getString(10) shouldBe "2015-05-16" // END_DATE
      firstLine.getString(11) shouldBe "2015-02-06" // LAST_UPDATE_DATE
      firstLine.getString(12) shouldBe "2005-03-18" // ENTRY_DATE
    }

    "read successor csv file" in {

      // When
      val result = AddressIndexFileReader.readSuccessorCSV().collect()

      // Then
      result.length shouldBe 3 // 4 with the header

      val firstLine = result(0)

      firstLine.getString(0) shouldBe "30" // RECORD_IDENTIFIER
      firstLine.getString(1) shouldBe "I" // CHANGE_TYPE
      firstLine.getString(2) shouldBe "12345" // PRO_ORDER
      firstLine.getString(3) shouldBe "100105677917" // UPRN
      firstLine.getString(4) shouldBe "9078S000000011" // SUCC_KEY
      firstLine.getString(5) shouldBe "2006-11-11" // START_DATE
      firstLine.getString(6) shouldBe "2027-11-15" // END_DATE
      firstLine.getString(7) shouldBe "2007-11-15" // LAST_UPDATE_DATE
      firstLine.getString(8) shouldBe "2006-10-10" // ENTRY_DATE
      firstLine.getString(9) shouldBe "122000001" // SUCCESSOR
    }
  }
}