package uk.gov.ons.addressindex.readers

import org.scalatest.{FlatSpec, Matchers}

class AddressIndexFileReaderSpec extends FlatSpec with Matchers {
  "AddressIndexFileReader" should
    "read delivery point csv file" in {
    // Given
    val expectedFirstDeliveryPoint = List("28", "I", "272650", "1", "19", "CIBO", "department", "FLAT E",
      "COTTAGE", "1", "throughfare", "SOME_STREET", "locality", "STIXTON", "LONDON", "POSTCODE", "S", "1Q", "welsh1",
      "welsh2", "welsh3", "welsh4", "welsh5", "welsh6", "2016-01-18", "2012-04-23", "2012-04-25",
      "2016-02-10", "2012-03-19")


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
}
