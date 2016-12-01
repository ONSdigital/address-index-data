package uk.gov.ons.addressindex.utils

import com.sun.xml.internal.ws.resources.AddressingMessages
import org.scalatest.{FlatSpec, Matchers}
import uk.gov.ons.addressindex.readers.AddressIndexFileReader

/**
  * Test that the csv files are joined correctly.
  */
class SqlHelperSpec extends FlatSpec with Matchers {

  "SqlHelper" should
    "join blpu, organisation, lpi, street and street_descriptor" in {

    // When
    val result = SqlHelper.joinCsvs(AddressIndexFileReader.readBlpuCSV(), AddressIndexFileReader.readLpiCSV(),
      AddressIndexFileReader.readOrganisationCSV(), AddressIndexFileReader.readStreetCSV(),
      AddressIndexFileReader.readStreetDescriptorCSV()).collect()

    // Then
    result.length shouldBe 1

    val firstLine = result(0)

    firstLine.getString(0) shouldBe "100010971564" // UPRN
    firstLine.getString(1) shouldBe "SOME COUNCIL" // ORGANISATION
    firstLine.getString(2) shouldBe "ANOTHER BUILDING NAME OR DESCRIPTION" // PAO_TEXT
    firstLine.getString(3) shouldBe "15" // PAO_START_NUMBER
    firstLine.getString(4) shouldBe "A BUILDING NAME OR DESCRIPTION" // SAO_TEXT
    firstLine.getString(5) shouldBe "A STREET DESCRIPTOR" // STREET_DESCRIPTOR
    firstLine.getString(6) shouldBe "TOWNY TOWN" // TOWN_NAME
    firstLine.getString(7) shouldBe "A GREAT LOCALITY" // LOCALITY
  }
}