package uk.gov.ons.addressindex.utils

import org.scalatest.{Matchers, WordSpec}
import uk.gov.ons.addressindex.readers.AddressIndexFileReader

/**
  * Test that the csv files are joined correctly.
  */
class SqlHelperSpec extends WordSpec with Matchers {

  "SqlHelper" should {
    "join blpu, organisation, lpi, street and street_descriptor" in {

      // When
      val result = SqlHelper.joinCsvs(AddressIndexFileReader.readBlpuCSV(), AddressIndexFileReader.readLpiCSV(),
        AddressIndexFileReader.readOrganisationCSV(), AddressIndexFileReader.readClassificationCSV(),
        AddressIndexFileReader.readStreetCSV(), AddressIndexFileReader.readStreetDescriptorCSV()).collect()

      // Then
      result.length shouldBe 4

      val firstLine = result(1)

      firstLine.getString(0) shouldBe "100010971564" // UPRN
      firstLine.getString(1) shouldBe "KL8 1JQ" // POSTCODE_LOCATOR
      firstLine.getString(2) shouldBe "D" // ADDRESSBASE_POSTAL
      firstLine.getString(3) shouldBe "53.6111710" // LATITUDE
      firstLine.getString(4) shouldBe "-2.3158117" // LONGITUDE
      firstLine.getString(5) shouldBe "379203.00" // X_COORDINATE
      firstLine.getString(6) shouldBe "412780.00" // Y_COORDINATE
      firstLine.getString(7) shouldBe "SOME COUNCIL" // ORGANISATION
      firstLine.getString(8) shouldBe "THE LEGAL NAME" // LEGAL_NAME
      firstLine.getString(9) shouldBe "RD" // CLASSIFICATION_CODE
      firstLine.getString(10) shouldBe "9401385" // USRN
      firstLine.getString(11) shouldBe "1610L000015314" // LPI_KEY
      firstLine.getString(12) shouldBe "ANOTHER BUILDING NAME OR DESCRIPTION" // PAO_TEXT
      firstLine.getString(13) shouldBe "15" // PAO_START_NUMBER
      firstLine.getString(14) shouldBe "CC" // PAO_START_SUFFIX
      firstLine.getString(15) shouldBe "9876" // PAO_END_NUMBER
      firstLine.getString(16) shouldBe "AB" // PAO_END_SUFFIX
      firstLine.getString(17) shouldBe "A BUILDING NAME OR DESCRIPTION" // SAO_TEXT
      firstLine.getString(18) shouldBe "1234" // SAO_START_NUMBER
      firstLine.getString(19) shouldBe "AA" // SAO_START_SUFFIX
      firstLine.getString(20) shouldBe "5678" // SAO_END_NUMBER
      firstLine.getString(21) shouldBe "BB" // SAO_END_SUFFIX
      firstLine.getString(22) shouldBe "VERTICAL POSITION" // LEVEL
      firstLine.getString(23) shouldBe "Y" // OFFICIAL_FLAG
      firstLine.getString(24) shouldBe "1" // LOGICAL_STATUS
      firstLine.getString(25) shouldBe "A STREET DESCRIPTOR" // STREET_DESCRIPTOR
      firstLine.getString(26) shouldBe "TOWNY TOWN" // TOWN_NAME
      firstLine.getString(27) shouldBe "A GREAT LOCALITY" // LOCALITY
    }

    "join blpu, organisation, lpi, street and street_descriptor for English and Welsh address" in {

      // When
      val result = SqlHelper.joinCsvs(AddressIndexFileReader.readBlpuCSV(), AddressIndexFileReader.readLpiCSV(),
        AddressIndexFileReader.readOrganisationCSV(), AddressIndexFileReader.readClassificationCSV(),
        AddressIndexFileReader.readStreetCSV(), AddressIndexFileReader.readStreetDescriptorCSV()).collect()

      // Then
      result.length shouldBe 4

      val firstLine = result(0)

      firstLine.getString(0) shouldBe "100010971565" // UPRN
      firstLine.getString(10) shouldBe "9402538" // USRN
      firstLine.getString(27) shouldBe "FSDF DSFSDF DSF" // LOCALITY

      val secondLine = result(3)

      secondLine.getString(0) shouldBe "100010971565" // UPRN
      secondLine.getString(10) shouldBe "9402538" // USRN
      secondLine.getString(27) shouldBe "LOCALITY XYZ" // LOCALITY
    }
  }
}