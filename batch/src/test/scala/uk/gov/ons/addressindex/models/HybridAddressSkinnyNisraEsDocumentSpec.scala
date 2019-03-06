package uk.gov.ons.addressindex.models

import org.apache.spark.sql.Row
import org.scalatest.{Matchers, WordSpec}

class HybridAddressSkinnyNisraEsDocumentSpec extends WordSpec with Matchers {

  val format = new java.text.SimpleDateFormat("yyyy-MM-dd")

  // Expected Paf Values
  val expectedPafUprn = 1
  val expectedPafEndDate = new java.sql.Date(format.parse("2012-04-25").getTime)
  val expectedPafStartDate = new java.sql.Date(format.parse("2012-04-23").getTime)
  val expectedPafAll = "DEPARTMENT CIBO FLAT E COTTAGE 6 1 THROUGHFARE WELSH1 SOME STREET WELSH2 LOCALITY WELSH3 STIXTON WELSH4 LONDON WELSH5 POSTCODE"
  val expectedPafMixed = "Department, Cibo, Flat E, Cottage, PO BOX 6, 1 Throughfare, Some Street, Locality, Stixton, London, POSTCODE"
  val expectedPafWelshMixed = "Department, Cibo, Flat E, Cottage, PO BOX 6, 1 Welsh1, Welsh2, Welsh3, Welsh4, Welsh5, POSTCODE"

  // Actual Paf Values
  val actualPafBuildingNumber = 1.toShort
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
  val actualPafRecordIdentifier = 27.toByte

  // Expected Nag values
  val expectedNagUprn = 100010971565L
  val expectedNagPostcodeLocator = "KL8 7HQ"
  val expectedNagAddressBasePostal = "D"
  val expectedNagEasting = 379171.00F
  val expectedNagNorthing = 412816.00F
  val expectedNagParentUprn = 999910971564L
  val expectedNagSaoEndSuffix = "JJ"
  val expectedNagPaoStartNumber = 56.toShort
  val expectedNagPaoStartSuffix = "HH"
  val expectedNagSaoStartNumber = 6473.toShort
  val expectedNagLpiLogicalStatus = 1.toByte
  val expectedNagStreetDescriptor = "And Another Street Descriptor"
  val expectedNagAll = "SOMETHING ELSE 6473FF-6623JJ THE BUILDING NAME A TRAINING CENTRE 56HH-7755OP AND ANOTHER STREET DESCRIPTOR LOCALITY XYZ TOWN B KL8 7HQ"
  val expectedNagLpiStartDate = new java.sql.Date(format.parse("2012-04-23").getTime)
  val expectedNagLpiEndDate = new java.sql.Date(format.parse("2018-01-11").getTime)
  val expectedNagMixed = "Something Else, 6473FF-6623JJ, The Building Name, A Training Centre, 56HH-7755OP And Another Street Descriptor, Locality Xyz, Town B, KL8 7HQ"

  // Actual Nag values
  val actualNagOrganisation = "SOMETHING ELSE"
  val actualNagOfficialFlag = "Y"
  val actualNagPaoStartNumber = 56.toShort
  val actualNagPostcodeLocator = "KL8 7HQ"
  val actualNagSaoEndSuffix = "JJ"
  val actualNagSaoStartNumber = 6473.toShort
  val actualNagUsrn = 9402538
  val actualNagLpiLogicalStatus = 1.toByte
  val actualNagEasting = 379171.00F
  val actualNagPaoEndSuffix = "OP"
  val actualNagStreetDescriptor = "AND ANOTHER STREET DESCRIPTOR"
  val actualNagUprn = 100010971565L
  val actualNagNorthing = 412816.00F
  val actualNagLpiKey = "1610L000014429"
  val actualNagSaoEndNumber = 6623.toShort
  val actualNagPaoEndNumber = 7755.toShort
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
  val actualNagMultiOccCount = 0.toShort
  val actualNagBlpuLogicalStatus = 1.toByte
  val actualNagLocalCustodianCode = 4218.toShort
  val actualNagRpc = 1.toByte
  val actualNagUsrnMatchIndicator = 1.toByte
  val actualNagLanguage = "ENG"
  val actualNagStreetClassification = 8.toByte
  val actualNagLpiStartDate = new java.sql.Date(format.parse("2012-04-23").getTime)
  val actualNagLpiLastUpdateDate = new java.sql.Date(format.parse("2012-04-24").getTime)
  val actualNagLpiEndDate = new java.sql.Date(format.parse("2018-01-11").getTime)

  // NISRA example
  val nisraOrganisation = "AN ORGANISATION"
  val nisraSubBuildingName = "THE SUB BUILDING NAME"
  val nisraBuildingName = "THE BUILDING NAME"
  val nisraBuildingNumber = "1A"
  val nisraThoroughfare = "THOROUGHFARE ROAD"
  val nisraDependentThoroughfare = "OFF HERE"
  val nisraAltThoroughfare = "AN ALTERNATIVE NAME"
  val nisraLocality = "A LOCALITY XYZ"
  val nisraTownland = "BIG TOWNLAND"
  val nisraTown = "LITTLE TOWN"
  val nisraPostCode = "AB1 7GH"
  val nisraEasting = 379171.00F
  val nisraNorthing = 412816.00F
  val nisraLocation = Array(-2.3162985F, 4.00F)
  val nisraUprn = 100010977866L
  val nisraCreationDate = new java.sql.Date(format.parse("2012-04-23").getTime)
  val nisraCommencementDate = new java.sql.Date(format.parse("2012-04-24").getTime)
  val nisraArchivedDate = new java.sql.Date(format.parse("2018-01-11").getTime)
  val nisraMixed = "An Organisation, The Sub Building Name, The Building Name, 1A Off Here, Thoroughfare Road, A Locality Xyz, Big Townland, Little Town, AB1 7GH"
  val nisraAltMixed = "An Organisation, The Sub Building Name, The Building Name, 1A Off Here, An Alternative Name, A Locality Xyz, Big Townland, Little Town, AB1 7GH"
  val nisraAll = "AN ORGANISATION THE SUB BUILDING NAME THE BUILDING NAME 1A OFF HERE THOROUGHFARE ROAD AN ALTERNATIVE NAME A LOCALITY XYZ BIG TOWNLAND LITTLE TOWN AB1 7GH"

  // used by both expected and actual to avoid assertion error
  val nagLocation = Array(-2.3162985F, 4.00F)

  val expectedPaf = Map(
    "endDate" -> expectedPafEndDate,
    "uprn" -> expectedPafUprn,
    "startDate" -> expectedPafStartDate,
    "pafAll" -> expectedPafAll,
    "mixedPaf" -> expectedPafMixed,
    "mixedWelshPaf" -> expectedPafWelshMixed
  )

  val expectedNag = Map(
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
    "nagAll" -> expectedNagAll,
    "lpiStartDate" -> expectedNagLpiStartDate,
    "lpiEndDate" -> expectedNagLpiEndDate,
    "mixedNag" -> expectedNagMixed
  )

  val expectedNisra = Map(
    "uprn" -> nisraUprn,
    "location" -> nisraLocation,
    "postcode" -> nisraPostCode,
    "easting" -> nisraEasting,
    "northing" -> nisraNorthing,
    "mixedNisra" -> nisraMixed,
    "creationDate" -> nisraCreationDate,
    "commencementDate" -> nisraCommencementDate,
    "archivedDate" -> nisraArchivedDate,
    "buildingNumber" -> nisraBuildingNumber,
    "mixedAltNisra" -> nisraAltMixed,
    "nisraAll" -> nisraAll
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
        actualNagLpiEndDate
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
        actualPafStartDate,
        actualPafEndDate,
        actualPafLastUpdateDate,
        actualPafEntryDate
      )

      // When
      val actual = HybridAddressSkinnyEsDocument.rowToPaf(row)

      // Then
      actual shouldBe expectedPaf
    }

    "Hybrid Address Skinny Elastic Search Document" should {

      "cast DataFrame's rows to a NISRA key-value Map" in {
        // Given
        val row = Row(
          nisraUprn,
          nisraOrganisation,
          nisraSubBuildingName,
          nisraBuildingName,
          nisraBuildingNumber,
          nisraThoroughfare,
          nisraAltThoroughfare,
          nisraDependentThoroughfare,
          nisraLocality,
          nisraTownland,
          nisraTown,
          nisraPostCode,
          nisraEasting,
          nisraNorthing,
          nisraLocation,
          nisraCreationDate,
          nisraCommencementDate,
          nisraArchivedDate,
          nisraMixed,
          nisraAltMixed,
          nisraAll
        )

        // When
        val actual = HybridAddressSkinnyNisraEsDocument.rowToNisra(row)

        // Then
        actual shouldBe expectedNisra
      }
    }

    "create NISRA with expected formatted address" in {

      // Also tests nisraAll
      // When
      val result = HybridAddressSkinnyNisraEsDocument.generateFormattedNisraAddresses(nisraOrganisation, nisraSubBuildingName,
        nisraBuildingName, nisraBuildingNumber, nisraThoroughfare, "", nisraDependentThoroughfare, nisraLocality,
        nisraTownland, nisraTown, nisraPostCode)

      val expected = nisraMixed
      val expectedNisraAll = "AN ORGANISATION THE SUB BUILDING NAME THE BUILDING NAME 1A OFF HERE THOROUGHFARE ROAD A LOCALITY XYZ BIG TOWNLAND LITTLE TOWN AB1 7GH"

      // Then
      result(0) shouldBe expected
      result(2) shouldBe expectedNisraAll
    }

    "create NISRA with expected formatted address (Alt Thoroughfare)" in {

      // Also tests nisraAll
      // When
      val result = HybridAddressSkinnyNisraEsDocument.generateFormattedNisraAddresses(nisraOrganisation, nisraSubBuildingName,
        nisraBuildingName, nisraBuildingNumber, nisraThoroughfare, nisraAltThoroughfare, nisraDependentThoroughfare, nisraLocality,
        nisraTownland, nisraTown, nisraPostCode)

      val expected = nisraAltMixed
      val expectedNisraAll = nisraAll

      // Then
      result(1) shouldBe expected
      result(2) shouldBe expectedNisraAll
    }
  }
}
