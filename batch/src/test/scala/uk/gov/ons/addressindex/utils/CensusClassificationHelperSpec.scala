package uk.gov.ons.addressindex.utils

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CensusClassificationHelperSpec extends AnyWordSpec with Matchers {

    val expectedEstabType1= "Hotel"
    val expectedEstabType2 = "Household"
    val expectedAddressType1 = "HH"
    val expectedAddressType2 = "SPG"

    "CensusClassificationHelper" should {
      "return the correct address Type with CouncilTax true" in {

        // Given
        val ABPcodeIn = "RD07"
        val councilTaxIn = true
        val nonDomesticRatesIn = false

        // When
        val result = CensusClassificationHelper.ABPToAddressType(ABPcodeIn, councilTaxIn,nonDomesticRatesIn)

        // Then
        result shouldBe expectedAddressType1

      }

      "return the correct address Type with CouncilTax false" in {

        // Given
        val ABPcodeIn = "RD01"
        val councilTaxIn = false
        val nonDomesticRatesIn = false

        // When
        val result = CensusClassificationHelper.ABPToAddressType(ABPcodeIn, councilTaxIn,nonDomesticRatesIn)

        // Then
        result shouldBe expectedAddressType2

      }

      "return the correct estab Type with NonDomesticRates true" in {

        // Given
        val ABPcodeIn = "CH01"
        val councilTaxIn = false
        val nonDomesticRatesIn = true
        // When
        val result = CensusClassificationHelper.ABPToEstabType(ABPcodeIn, councilTaxIn,nonDomesticRatesIn)

        // Then
        result shouldBe expectedEstabType1

      }

      "return the correct estab Type with NonDomesticRates false" in {

        // Given
        val ABPcodeIn = "CH01"
        val councilTaxIn = false
        val nonDomesticRatesIn = false
        // When
        val result = CensusClassificationHelper.ABPToEstabType(ABPcodeIn, councilTaxIn,nonDomesticRatesIn)

        // Then
        result shouldBe expectedEstabType2

      }
    }

}
