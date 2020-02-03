package uk.gov.ons.addressindex.utils

import org.scalatest.{Matchers, WordSpec}

class CensusClassificationHelperSpec extends WordSpec with Matchers {

    val expectedEstabType = "Household"
    val expectedAddressType = "HH"

    "CensusClassificationHelper" should {
      "return the correct address Type" in {

        // Given
        val ABPcodeIn = "HH"

        // When
        val result = CensusClassificationHelper.ABPToAddressType(ABPcodeIn)

        // Then
        result shouldBe expectedAddressType

      }

      "return the correct estab Type" in {

        // Given
        val ABPcodeIn = "Household"

        // When
        val result = CensusClassificationHelper.ABPToEstabType(ABPcodeIn)

        // Then
        result shouldBe expectedEstabType

      }
    }

}
