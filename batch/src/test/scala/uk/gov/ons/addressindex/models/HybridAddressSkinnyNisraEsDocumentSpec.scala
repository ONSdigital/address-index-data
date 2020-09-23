package uk.gov.ons.addressindex.models

import org.apache.spark.sql.Row
import org.scalatest.{Matchers, WordSpec}

class HybridAddressSkinnyNisraEsDocumentSpec extends WordSpec with Matchers {

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
  val expectedNagLanguage = "ENG"
  val expectedNagAll = "SOMETHING ELSE 6473FF-6623JJ THE BUILDING NAME A TRAINING CENTRE 56HH-7755OP AND ANOTHER STREET DESCRIPTOR LOCALITY XYZ TOWN B KL8 7HQ"
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

  // NISRA location (shared field to avoid test failure)
  val nisraLocation = Array(-2.3162985F, 4.00F)

  val expectedNisraBuildingNumber: Null = null
  val expectedNisraPostCode = "AB1 7GH"
  val expectedNisraThoroughfare = "Thoroughfare Road"
  val expectedNisraTownland = "Big Townland"
  val expectedNisraTownName = "Little Town"
  val expectedNisraEasting = 379171.00F
  val expectedNisraNorthing = 412816.00F
  val expectedNisraUprn = 100010977866L
  val expectedNisraPaoStartNumber = 1
  val expectedNisraSaoStartNumber = 1
  val expectedNisraMixed = "An Organisation, The Sub Building Name, The Building Name, 1A Thoroughfare Road, Off Here, A Locality Xyz, Little Town, AB1 7GH AB17GH"
  val expectedNisraMixedStart = "An Organisat"
  val expectedNisraAltMixed = "An Organisation, The Sub Building Name, The Building Name, 1A, An Alternative Name, Off Here, A Locality Xyz, Little Town, AB1 7GH AB17GH"
  val expectedNisraAll = "AN ORGANISATION THE SUB BUILDING NAME THE BUILDING NAME 1A THOROUGHFARE ROAD OFF HERE AN ALTERNATIVE NAME A LOCALITY XYZ LITTLE TOWN AB1 7GH AB17GH"
  val expectedNisraClassificationCode = "RD06"
  val expectedNisraLocalCustodianCode = "N09000002"
  val expectedNisraSecondarySort = "THE BUILDING NAME 0001 THE SUB BUILDING NAME AN ORGANISATION"
  // used by both expected and actual to avoid assertion error
  val nagLocation = Array(-2.3162985F, 4.00F)

  val expectedPaf: Map[String, Any] = Map(
    "uprn" -> expectedPafUprn,
    "postTown" -> expectedPafPostTown,
    "thoroughfare" -> expectedPafThoroughFare,
    "mixedPaf" -> expectedPafMixed,
    "mixedWelshPaf" -> expectedPafWelshMixed,
    "mixedPafStart" -> expectedPafMixedStart,
    "mixedWelshPafStart" -> expectedPafWelshMixedStart
  )

  val expectedNag: Map[String, Any] = Map(
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

    // NISRA actual
  val actualNisraOrganisation = "AN ORGANISATION"
  val actualNisraSubBuildingName = "THE SUB BUILDING NAME"
  val actualNisraBuildingName = "THE BUILDING NAME"
  val actualNisraBuildingNumber = "1A"
  val actualNisraThoroughfare = "THOROUGHFARE ROAD"
  val actualNisraDependentThoroughfare = "OFF HERE"
  val actualNisraAltThoroughfare = "AN ALTERNATIVE NAME"
  val actualNisraLocality = "A LOCALITY XYZ"
  val actualNisraTownland = "BIG TOWNLAND"
  val actualNisraTown = "LITTLE TOWN"
  val actualNisraCounty = "A COUNTY"
  val actualNisraPostCode = "AB1 7GH"
  val actualNisraPostTown = "A POST TOWN"
  val actualNisraAddressLine1 = "ADDRESS LINE 1"
  val actualNisraAddressLine2 = "ADDRESS LINE 2"
  val actualNisraAddressLine3 = "ADDRESS LINE 3"
  val actualNisraRegion = "N92000002"
  val actualNisraLad = "N09000004"
  val actualNisraXCoordinate = 379171.00F
  val actualNisraYCoordinate = 412816.00F
  val actualNisraTempCoords = "Y"
  val actualNisraUprn = 100010977866L
  val actualNisraUdprn = 12345
  val actualNisraUsrn = 12345
  val actualNisraBuildingStatus = "WONKY"
  val actualNisraAddressType = "HH"
  val actualNisraEstabType = "HOUSEHOLD"
  val actualNisraRecordIdentifier: Byte = 27.toByte
  val actualNisraParentUprn = 999910971564L
  val actualNisraPrimaryUprn = 999911111111L
  val actualNisraSecondaryUprn = "NA"
  val actualNisraThisLayer = 1
  val actualNisraLayers = 1
  val actualNisraNodeType = "SINGLETON"
  val actualNisraAddress1YearAgo = "ADDRESS 1 YEAR AGO"
  val actualNisraClassificationCode = "RD06"
  val actualNisraClassification = "DO_APART"
  val actualNisraMixed = "An Organisation, The Sub Building Name, The Building Name, 1A Thoroughfare Road, Off Here, A Locality Xyz, Big Townland, Little Town, AB1 7GH AB17GH"
  val actualNisraAltMixed = "An Organisation, The Sub Building Name, The Building Name, 1A, An Alternative Name, Off Here, A Locality Xyz, Big Townland, Little Town, AB1 7GH AB17GH"
  val actualNisraBuildingNumMixed = "1 Thoroughfare Road, A Locality Xyz, Big Townland, Little Town, AB1 7GH"
  val actualNisraBuildingNameMixed = "1A Thoroughfare Road, A Locality Xyz, Big Townland, Little Town, AB1 7GH"
  val actualNisraAll = "AN ORGANISATION THE SUB BUILDING NAME THE BUILDING NAME 1A THOROUGHFARE ROAD OFF HERE AN ALTERNATIVE NAME A LOCALITY XYZ BIG TOWNLAND LITTLE TOWN AB1 7GH AB17GH"
  val actualNisraCreationDate = new java.sql.Date(format.parse("2012-04-23").getTime)
  val actualNisraCommencementDate = new java.sql.Date(format.parse("2012-04-24").getTime)
  val actualNisraArchivedDate = new java.sql.Date(format.parse("2018-01-11").getTime)
  val actualNisraComplete = "1"
  val actualNisraPaoText = "THE BUILDING NAME"
  val actualNisraPaoStartNumber = "1"
  val actualNisraPaoStartSuffix = "A"
  val actualNisraPaoEndNumber = ""
  val actualNisraPaoEndSuffix = ""
  val actualNisraSaoText = "THE SUB BUILDING NAME"
  val actualNisraSaoStartNumber  = "1"
  val actualNisraSaoStartSuffix = ""
  val actualNisraSaoEndNumber = ""
  val actualNisraSaoEndSuffix = ""
  val actualNisraLocalCouncil = "BELFAST"
  val actualNisraLocalCustodianCode = "N09000002"
  val actualNisraLGDCode = "N09000003"
  val actualNisraBlpuCode: Byte = 1.toByte
  val actualNisraLogicalStatus: Byte = 1.toByte

  val expectedNisra: Map[String, Any] = Map(
    "uprn" -> expectedNisraUprn,
    "location" -> nisraLocation,
    "postcode" -> expectedNisraPostCode,
    "townName" -> expectedNisraTownName,
    "thoroughfare" -> expectedNisraThoroughfare,
    "easting" -> expectedNisraEasting,
    "northing" -> expectedNisraNorthing,
    "mixedNisra" -> expectedNisraMixed,
    "paoStartNumber" -> expectedNisraPaoStartNumber,
    "saoStartNumber" -> expectedNisraSaoStartNumber,
    "classificationCode" -> expectedNisraClassificationCode,
    "buildingNumber" -> expectedNisraBuildingNumber,
    "mixedAltNisra" -> expectedNisraAltMixed,
    "localCustodianCode"-> expectedNisraLocalCustodianCode,
    "nisraAll" -> expectedNisraAll,
    "mixedNisraStart" -> expectedNisraMixedStart,
    "secondarySort" -> expectedNisraSecondarySort
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
          actualNisraUprn,
          actualNisraSubBuildingName,
          actualNisraBuildingName,
          actualNisraBuildingNumber,
          actualNisraPaoStartNumber,
          actualNisraPaoEndNumber,
          actualNisraPaoStartSuffix,
          actualNisraPaoEndSuffix,
          actualNisraPaoText,
          actualNisraSaoStartNumber,
          actualNisraSaoEndNumber,
          actualNisraSaoStartSuffix,
          actualNisraSaoEndSuffix,
          actualNisraSaoText,
          actualNisraOrganisation,
          actualNisraThoroughfare,
          actualNisraAltThoroughfare,
          actualNisraDependentThoroughfare,
          actualNisraLocality,
          actualNisraUdprn,
          actualNisraTown,
          actualNisraPostCode,
          actualNisraXCoordinate,
          actualNisraYCoordinate,
          nisraLocation,
          actualNisraCreationDate,
          actualNisraCommencementDate,
          actualNisraArchivedDate,
          actualNisraClassificationCode,
          actualNisraTownland,
          actualNisraCounty,
          actualNisraLocalCustodianCode,
          actualNisraBlpuCode,
          actualNisraLogicalStatus,
          actualNisraAddressType,
          actualNisraEstabType,
          actualNisraLad,
          actualNisraRegion,
          actualNisraRecordIdentifier,
          actualNisraParentUprn,
          actualNisraUsrn,
          actualNisraPrimaryUprn,
          actualNisraSecondaryUprn,
          actualNisraThisLayer,
          actualNisraLayers,
          actualNisraNodeType,
          actualNisraAddressLine1,
          actualNisraAddressLine2,
          actualNisraAddressLine3,
          actualNisraTempCoords,
          actualNisraAddress1YearAgo,
          actualNisraClassification,
          actualNisraTown
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
      val result = HybridAddressSkinnyNisraEsDocument.generateFormattedNisraAddresses(actualNisraOrganisation, actualNisraSubBuildingName,
        actualNisraBuildingName, actualNisraBuildingNumber, actualNisraThoroughfare, "", actualNisraDependentThoroughfare, actualNisraLocality,
        "", actualNisraTown, actualNisraPostCode + " " + actualNisraPostCode.replaceAll(" ",""))

      val expected = expectedNisraMixed
      val expectedAll = "AN ORGANISATION THE SUB BUILDING NAME THE BUILDING NAME 1A THOROUGHFARE ROAD OFF HERE A LOCALITY XYZ LITTLE TOWN AB1 7GH AB17GH"

      // Then
      result(0) shouldBe expected
      result(2) shouldBe expectedAll
    }

    "create NISRA with expected formatted address (Alt Thoroughfare)" in {

      // Also tests nisraAll
      // When
      val result = HybridAddressSkinnyNisraEsDocument.generateFormattedNisraAddresses(actualNisraOrganisation, actualNisraSubBuildingName,
        actualNisraBuildingName, actualNisraBuildingNumber, actualNisraThoroughfare, actualNisraAltThoroughfare, actualNisraDependentThoroughfare, actualNisraLocality,
        "", actualNisraTown, actualNisraPostCode + " " + actualNisraPostCode.replaceAll(" ",""))

      val expected = expectedNisraAltMixed
      val expectedAll = expectedNisraAll

      // Then
      result(1) shouldBe expected
      result(2) shouldBe expectedAll
    }

    "create NISRA with expected formatted address (number and thoroughfare)" in {

      // When
      val result = HybridAddressNisraEsDocument.generateFormattedNisraAddresses(
        "",
        "",
        "",
        "1",
        actualNisraThoroughfare,
        "",
        "",
        actualNisraLocality,
        actualNisraTownland,
        actualNisraTown,
        actualNisraPostCode)

      val expected =  actualNisraBuildingNumMixed

      // Then
      result(0) shouldBe expected
    }


    "create NISRA with expected formatted address (part-numeric building and thoroughfare)" in {

      // Also tests nisraAll
      // When
      val result = HybridAddressNisraEsDocument.generateFormattedNisraAddresses(
        "",
        "",
        "1A",
        "",
        actualNisraThoroughfare,
        "",
        "",
        actualNisraLocality,
        actualNisraTownland,
        actualNisraTown,
        actualNisraPostCode)

      val expected =  actualNisraBuildingNameMixed

      // Then
      result(0) shouldBe expected
    }
  }
}
