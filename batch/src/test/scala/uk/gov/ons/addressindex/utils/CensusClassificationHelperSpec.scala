package uk.gov.ons.addressindex.utils

import org.scalatest.{Matchers, WordSpec}

class CensusClassificationHelperSpec extends WordSpec with Matchers {

    val expectedEstabType = "NA"
    val expectedAddressType1 = "AAAA"
    val expectedAddressType2 = "BBBB"

    "CensusClassificationHelper" should {
      "return the correct address Type with CouncilTax true" in {

        // Given
        val ABPcodeIn = "RD07"
        val councilTaxIn = true

        // When
        val result = CensusClassificationHelper.ABPToAddressType(ABPcodeIn, councilTaxIn)

        // Then
        result shouldBe expectedAddressType1

      }

      "return the correct address Type with CouncilTax false" in {

        // Given
        val ABPcodeIn = "RD01"
        val councilTaxIn = false

        // When
        val result = CensusClassificationHelper.ABPToAddressType(ABPcodeIn, councilTaxIn)

        // Then
        result shouldBe expectedAddressType2

      }

      "return the correct estab Type" in {

        // Given
        val ABPcodeIn = "Household"
        val councilTaxIn = true
        // When
        val result = CensusClassificationHelper.ABPToEstabType(ABPcodeIn, councilTaxIn)

        // Then
        result shouldBe expectedEstabType

      }
    }

}
