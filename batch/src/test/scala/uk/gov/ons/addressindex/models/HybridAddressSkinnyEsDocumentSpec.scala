package uk.gov.ons.addressindex.models

import org.apache.spark.sql.Row
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

class HybridAddressSkinnyEsDocumentSpec extends AnyWordSpec with Matchers {

  val format = new java.text.SimpleDateFormat("yyyy-MM-dd")

  // Expected Paf Values
  val expectedPafUprn = 1
  val expectedPafThoroughFare ="Some Street"
  val expectedPafPostTown = "London"
  val expectedPafEndDate = new java.sql.Date(format.parse("2012-04-25").getTime)
  val expectedPafStartDate = new java.sql.Date(format.parse("2012-04-23").getTime)
  val expectedPafAll = "DEPARTMENT CIBO FLAT E COTTAGE 6 1 THROUGHFARE WELSH1 SOME STREET WELSH2 LOCALITY WELSH3 STIXTON WELSH4 LONDON WELSH5 POSTCODE"
  val expectedPafMixed = "Department, Cibo, Flat E, Cottage, PO Box 6, 1 Throughfare, Some Street, Locality, Stixton, London, POSTCODE POSTCODE"
  val expectedPafWelshMixed = "Department, Cibo, Flat E, Cottage, PO Box 6, 1 Welsh1, Welsh2, Welsh3, Welsh4, Welsh5, POSTCODE POSTCODE"
  val expectedPafMixedStart = "Department C"
  val expectedPafWelshMixedStart = "Department C"

  // Actual Paf Values
  val actualPafBuildingNumber: Short = 1.toShort
  val actualPafUdprn = 1
  val actualPafLastUpdateDate = new java.sql.Date(format.parse("2016-02-10").getTime)
  val actualPafProOrder = 272650L
  val actualPafEndDate = new java.sql.Date(format.parse("2012-04-25").getTime)
  val actualPafPostcodeType = "S"
  val actualPafDependentThoroughfare = "THROUGHFARE"
  val actualPafEntryDate = new java.sql.Date(format.parse("2012-03-19").getTime)
  val actualPafWelshPostTown = "WELSH5"
  val actualPafDeliveryPointSuffix = "1Q"
  val actualPafPostcode = "POSTCODE"
  val actualPafProcessDate = new java.sql.Date(format.parse("2016-01-18").getTime)
  val actualPafPoBoxNumber = "6"
  val actualPafUprn = 1L
  val actualPafDependentLocality = "STIXTON"
  val actualPafBuildingName = "COTTAGE"
  val actualPafWelshDoubleDependentLocality = "WELSH3"
  val actualPafOrganisationName = "CIBO"
  val actualPafPostTown = "LONDON"
  val actualPafChangeType = "I"
  val actualPafDepartmentName = "DEPARTMENT"
  val actualPafWelshDependentLocality = "WELSH4"
  val actualPafDoubleDependentLocality = "LOCALITY"
  val actualPafWelshDependentThoroughfare = "WELSH1"
  val actualPafSubBuildingName = "FLAT E"
  val actualPafWelshThoroughfare = "WELSH2"
  val actualPafThoroughfare = "SOME STREET"
  val actualPafStartDate = new java.sql.Date(format.parse("2012-04-23").getTime)
  val actualPafRecordIdentifier: Byte = 27.toByte

  // Expected Nag values
  val expectedNagUprn = 100010971565L
  val expectedNagPostcodeLocator = "KL8 7HQ"
  val expectedNagAddressBasePostal = "D"
  val expectedNagEasting = 379171.00F
  val expectedNagNorthing = 412816.00F
  val expectedNagParentUprn = 999910971564L
  val expectedNagSaoEndSuffix = "JJ"
  val expectedNagPaoStartNumber: Short = 56.toShort
  val expectedNagPaoStartSuffix = "HH"
  val expectedNagSaoStartNumber: Short = 6473.toShort
  val expectedNagLpiLogicalStatus: Byte = 1.toByte
  val expectedNagStreetDescriptor = "And Another Street Descriptor"
  val expectedNagTownName = "Town B"
  val expectedNagAll = "SOMETHING ELSE 6473FF-6623JJ THE BUILDING NAME A TRAINING CENTRE 56HH-7755OP AND ANOTHER STREET DESCRIPTOR LOCALITY XYZ TOWN B KL8 7HQ"
  val expectedNagLanguage = "ENG"
  val expectedNagLpiStartDate = new java.sql.Date(format.parse("2012-04-23").getTime)
  val expectedNagLpiEndDate = new java.sql.Date(format.parse("2018-01-11").getTime)
  val expectedNagMixed = "Something Else, 6473FF-6623JJ, The Building Name, A Training Centre, 56HH-7755OP And Another Street Descriptor, Locality Xyz, Town B, KL8 7HQ KL87HQ"
  val expectedWelshNagMixed = ""
  val expectedMixedNagStart = "Something El"
  val expectedMixedWelshNagStart = ""
  val expectedNagSecondarySort = "A TRAINING CENTRE 6473FF SOMETHING ELSE THE BUILDING NAME"
  val expectedNagCountry = "E"
  val expectedNagLocality = "Locality Xyz"

  // Actual Nag values
  val actualNagOrganisation = "SOMETHING ELSE"
  val actualNagOfficialFlag = "Y"
  val actualNagPaoStartNumber: Short = 56.toShort
  val actualNagPostcodeLocator = "KL8 7HQ"
  val actualNagSaoEndSuffix = "JJ"
  val actualNagSaoStartNumber: Short = 6473.toShort
  val actualNagUsrn = 9402538
  val actualNagLpiLogicalStatus: Byte = 1.toByte
  val actualNagEasting = 379171.00F
  val actualNagPaoEndSuffix = "OP"
  val actualNagStreetDescriptor = "AND ANOTHER STREET DESCRIPTOR"
  val actualNagUprn = 100010971565L
  val actualNagNorthing = 412816.00F
  val actualNagLpiKey = "1610L000014429"
  val actualNagSaoEndNumber: Short = 6623.toShort
  val actualNagPaoEndNumber: Short = 7755.toShort
  val actualNagTownName = "TOWN B"
  val actualNagLegalName = "ANOTHER LEGAL NAME"
  val actualNagSaoStartSuffix = "FF"
  val actualNagPaoText = "A TRAINING CENTRE"
  val actualNagSaoText = "THE BUILDING NAME"
  val actualNagPaoStartSuffix = "HH"
  val actualNagAddressBasePostal = "D"
  val actualNagLocality = "LOCALITY XYZ"
  val actualNagLevel = "UP THERE SOME WHERE"
  val actualNagParentUprn = 999910971564L
  val actualNagMultiOccCount: Short = 0.toShort
  val actualNagBlpuLogicalStatus: Byte = 1.toByte
  val actualNagLocalCustodianCode: Short = 4218.toShort
  val actualNagRpc: Byte = 1.toByte
  val actualNagUsrnMatchIndicator: Byte = 1.toByte
  val actualNagLanguage = "ENG"
  val actualNagStreetClassification: Byte = 8.toByte
  val actualNagLpiStartDate = new java.sql.Date(format.parse("2012-04-23").getTime)
  val actualNagLpiLastUpdateDate = new java.sql.Date(format.parse("2012-04-24").getTime)
  val actualNagLpiEndDate = new java.sql.Date(format.parse("2018-01-11").getTime)
  val actualNagCountry = "E"

  // used by both expected and actual to avoid assertion error
  val nagLocation: Array[Float] = Array(-2.3162985F, 4.00F)

  val expectedPaf: Map[String, Any] = Map[String,Any](
    "uprn" -> expectedPafUprn,
    "postTown" -> expectedPafPostTown,
    "thoroughfare" -> expectedPafThoroughFare,
    "mixedPaf" -> expectedPafMixed,
    "mixedWelshPaf" -> expectedPafWelshMixed,
    "mixedPafStart" -> expectedPafMixedStart,
    "mixedWelshPafStart" -> expectedPafWelshMixedStart
  )

  val expectedNag: Map[String, Any] = Map[String,Any](
    "uprn" -> expectedNagUprn,
    "postcodeLocator" -> expectedNagPostcodeLocator,
    "addressBasePostal" -> expectedNagAddressBasePostal,
    "location" -> nagLocation,
    "easting" -> expectedNagEasting,
    "northing" -> expectedNagNorthing,
    "parentUprn" -> expectedNagParentUprn,
    "paoStartNumber" -> expectedNagPaoStartNumber,
    "paoStartSuffix" -> expectedNagPaoStartSuffix,
    "saoStartNumber" -> expectedNagSaoStartNumber,
    "lpiLogicalStatus" -> expectedNagLpiLogicalStatus,
    "streetDescriptor" -> expectedNagStreetDescriptor,
    "townName" -> expectedNagTownName,
    "nagAll" -> expectedNagAll,
    "language" -> expectedNagLanguage,
    "mixedNag" -> expectedNagMixed,
    "mixedWelshNag" -> expectedWelshNagMixed,
    "mixedNagStart" -> expectedMixedNagStart,
    "mixedWelshNagStart" -> expectedMixedWelshNagStart,
    "secondarySort" -> expectedNagSecondarySort,
    "country" -> expectedNagCountry,
    "locality" -> expectedNagLocality
  )

  "Hybrid Address Elastic Search Document" should {

    "cast DataFrame's rows to an LPI key-value Map" in {
      // Given
      val row = Row(
        actualNagUprn,
        actualNagPostcodeLocator,
        actualNagAddressBasePostal,
        nagLocation,
        actualNagEasting,
        actualNagNorthing,
        actualNagParentUprn,
        actualNagMultiOccCount,
        actualNagBlpuLogicalStatus,
        actualNagLocalCustodianCode,
        actualNagRpc,
        actualNagOrganisation,
        actualNagLegalName,
        actualNagUsrn,
        actualNagLpiKey,
        actualNagPaoText,
        actualNagPaoStartNumber,
        actualNagPaoStartSuffix,
        actualNagPaoEndNumber,
        actualNagPaoEndSuffix,
        actualNagSaoText,
        actualNagSaoStartNumber,
        actualNagSaoStartSuffix,
        actualNagSaoEndNumber,
        actualNagSaoEndSuffix,
        actualNagLevel,
        actualNagOfficialFlag,
        actualNagLpiLogicalStatus,
        actualNagUsrnMatchIndicator,
        actualNagLanguage,
        actualNagStreetDescriptor,
        actualNagTownName,
        actualNagLocality,
        actualNagStreetClassification,
        actualNagLpiStartDate,
        actualNagLpiLastUpdateDate,
        actualNagLpiEndDate,
        actualNagCountry
      )

      // When
      val actual = HybridAddressSkinnyEsDocument.rowToLpi(row)

      // Then
      actual shouldBe expectedNag
    }

    "cast DataFrame's rows to an PAF key-value Map" in {
      // Given
      val row = Row(
        actualPafRecordIdentifier,
        actualPafChangeType,
        actualPafProOrder,
        actualPafUprn,
        actualPafUdprn,
        actualPafOrganisationName,
        actualPafDepartmentName,
        actualPafSubBuildingName,
        actualPafBuildingName,
        actualPafBuildingNumber,
        actualPafDependentThoroughfare,
        actualPafThoroughfare,
        actualPafDoubleDependentLocality,
        actualPafDependentLocality,
        actualPafPostTown,
        actualPafPostcode,
        actualPafPostcodeType,
        actualPafDeliveryPointSuffix,
        actualPafWelshDependentThoroughfare,
        actualPafWelshThoroughfare,
        actualPafWelshDoubleDependentLocality,
        actualPafWelshDependentLocality,
        actualPafWelshPostTown,
        actualPafPoBoxNumber,
        actualPafProcessDate,
        actualPafEntryDate
      )

      // When
      val actual = HybridAddressSkinnyEsDocument.rowToPaf(row)

      // Then
      actual shouldBe expectedPaf
    }

  }
}
