package uk.gov.ons.addressindex.models

import org.apache.spark.sql.Row
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class HybridAddressEsDocumentSpec extends AnyWordSpec with Matchers {

  val format = new java.text.SimpleDateFormat("yyyy-MM-dd")

  // Expected Paf values
  val expectedPafBuildingNumber: Short = 1.toShort
  val expectedPafUdprn = 19
  val expectedPafLastUpdateDate = new java.sql.Date(format.parse("2016-02-10").getTime)
  val expectedPafProOrder = 272650L
  val expectedPafEndDate = new java.sql.Date(format.parse("2012-04-25").getTime)
  val expectedPafPostcodeType = "S"
  val expectedPafDependentThoroughfare = "Throughfare"
  val expectedPafEntryDate = new java.sql.Date(format.parse("2012-03-19").getTime)
  val expectedPafWelshPostTown = "Welsh5"
  val expectedPafDeliveryPointSuffix = "1Q"
  val expectedPafPostcode = "POSTCODE"
  val expectedPafProcessDate = new java.sql.Date(format.parse("2016-01-18").getTime)
  val expectedPafPoBoxNumber = "6"
  val expectedPafUprn = 1L
  val expectedPafDependentLocality = "Stixton"
  val expectedPafBuildingName = "Cottage"
  val expectedPafWelshDoubleDependentLocality = "Welsh3"
  val expectedPafOrganisationName = "Cibo"
  val expectedPafPostTown = "London"
  val expectedPafChangeType = "I"
  val expectedPafDepartmentName = "Department"
  val expectedPafWelshDependentLocality = "Welsh4"
  val expectedPafDoubleDependentLocality = "Locality"
  val expectedPafWelshDependentThoroughfare = "Welsh1"
  val expectedPafSubBuildingName = "Flat E"
  val expectedPafWelshThoroughfare = "Welsh2"
  val expectedPafThoroughfare = "Some Street"
  val expectedPafStartDate = new java.sql.Date(format.parse("2012-04-23").getTime)
  val expectedPafRecordIdentifier: Byte = 27.toByte
  val expectedPafAll = "DEPARTMENT CIBO FLAT E COTTAGE 6 1 THROUGHFARE WELSH1 SOME STREET WELSH2 LOCALITY WELSH3 STIXTON WELSH4 LONDON WELSH5 POSTCODE"
  val expectedPafMixed = "Department, Cibo, Flat E, Cottage, PO Box 6, 1 Throughfare, Some Street, Locality, Stixton, London, POSTCODE POSTCODE"
  val expectedPafWelshMixed = "Department, Cibo, Flat E, Cottage, PO Box 6, 1 Welsh1, Welsh2, Welsh3, Welsh4, Welsh5, POSTCODE POSTCODE"
  val expectedPafMixedStart = "Department C"
  val expectedPafWelshMixedStart = "Department C"

  // Actual Paf values
  val actualPafBuildingNumber: Short = 1.toShort
  val actualPafUdprn = 19
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
  val expectedNagOrganisation = "Something Else"
  val expectedNagOfficialFlag = "Y"
  val expectedNagPaoStartNumber: Short = 56.toShort
  val expectedNagPostcodeLocator = "KL8 7HQ"
  val expectedNagSaoEndSuffix = "JJ"
  val expectedNagSaoStartNumber: Short = 6473.toShort
  val expectedNagUsrn = 9402538
  val expectedNagLpiLogicalStatus: Byte = 1.toByte
  val expectedNagEasting = 379171.00F
  val expectedNagPaoEndSuffix = "OP"
  val expectedNagStreetDescriptor = "And Another Street Descriptor"
  val expectedNagUprn = 100010971565L
  val expectedNagNorthing = 412816.00F
  val expectedNagLpiKey = "1610L000014429"
  val expectedNagSaoEndNumber: Short = 6623.toShort
  val expectedNagPaoEndNumber: Short = 7755.toShort
  val expectedNagTownName = "Town B"
  val expectedNagLegalName = "ANOTHER LEGAL NAME"
  val expectedNagSaoStartSuffix = "FF"
  val expectedNagPaoText = "A Training Centre"
  val expectedNagSaoText = "The Building Name"
  val expectedNagPaoStartSuffix = "HH"
  val expectedNagAddressBasePostal = "D"
  val expectedNagLocality = "Locality Xyz"
  val expectedNagLevel = "UP THERE SOME WHERE"
  val expectedNagParentUprn = 999910971564L
  val expectedNagMultiOccCount: Short = 0.toShort
  val expectedNagBlpuLogicalStatus: Byte = 1.toByte
  val expectedNagLocalCustodianCode: Short = 4218.toShort
  val expectedNagRpc: Byte = 1.toByte
  val expectedNagUsrnMatchIndicator: Byte = 1.toByte
  val expectedNagLanguage = "ENG"
  val expectedNagStreetClassification: Byte = 8.toByte
  val expectedNagAll = "SOMETHING ELSE 6473FF-6623JJ THE BUILDING NAME A TRAINING CENTRE 56HH-7755OP AND ANOTHER STREET DESCRIPTOR LOCALITY XYZ TOWN B KL8 7HQ"
  val expectedNagLpiStartDate = new java.sql.Date(format.parse("2012-04-23").getTime)
  val expectedNagLpiLastUpdateDate = new java.sql.Date(format.parse("2012-04-24").getTime)
  val expectedNagLpiEndDate = new java.sql.Date(format.parse("2018-01-11").getTime)
  val expectedNagMixed = "Something Else, 6473FF-6623JJ, The Building Name, A Training Centre, 56HH-7755OP And Another Street Descriptor, Locality Xyz, Town B, KL8 7HQ KL87HQ"
  val expectedWelshNagMixed = ""
  val expectedMixedNagStart = "Something El"
  val expectedMixedWelshNagStart = ""
  val expectedNagSecondarySort = "A TRAINING CENTRE 6473FF SOMETHING ELSE THE BUILDING NAME"
  val expectedNagCountry = "E"

  // Actual Nag Values
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
    "buildingNumber" -> expectedPafBuildingNumber,
    "udprn" -> expectedPafUdprn,
    "lastUpdateDate" -> expectedPafLastUpdateDate,
    "proOrder" -> expectedPafProOrder,
    "endDate" -> expectedPafEndDate,
    "postcodeType" -> expectedPafPostcodeType,
    "dependentThoroughfare" -> expectedPafDependentThoroughfare,
    "entryDate" -> expectedPafEntryDate,
    "welshPostTown" -> expectedPafWelshPostTown,
    "deliveryPointSuffix" -> expectedPafDeliveryPointSuffix,
    "postcode" -> expectedPafPostcode,
    "processDate" -> expectedPafProcessDate,
    "poBoxNumber" -> expectedPafPoBoxNumber,
    "uprn" -> expectedPafUprn,
    "dependentLocality" -> expectedPafDependentLocality,
    "buildingName" -> expectedPafBuildingName,
    "welshDoubleDependentLocality" -> expectedPafWelshDoubleDependentLocality,
    "organisationName" -> expectedPafOrganisationName,
    "postTown" -> expectedPafPostTown,
    "changeType" -> expectedPafChangeType,
    "departmentName" -> expectedPafDepartmentName,
    "welshDependentLocality" -> expectedPafWelshDependentLocality,
    "doubleDependentLocality" -> expectedPafDoubleDependentLocality,
    "welshDependentThoroughfare" -> expectedPafWelshDependentThoroughfare,
    "subBuildingName" -> expectedPafSubBuildingName,
    "welshThoroughfare" -> expectedPafWelshThoroughfare,
    "thoroughfare" -> expectedPafThoroughfare,
    "startDate" -> expectedPafStartDate,
    "recordIdentifier" -> expectedPafRecordIdentifier,
    "pafAll" -> expectedPafAll,
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
    "multiOccCount" -> expectedNagMultiOccCount,
    "blpuLogicalStatus" -> expectedNagBlpuLogicalStatus,
    "localCustodianCode" -> expectedNagLocalCustodianCode,
    "rpc" -> expectedNagRpc,
    "organisation" -> expectedNagOrganisation,
    "legalName" -> expectedNagLegalName,
    "usrn" -> expectedNagUsrn,
    "lpiKey" -> expectedNagLpiKey,
    "paoText" -> expectedNagPaoText,
    "paoStartNumber" -> expectedNagPaoStartNumber,
    "paoStartSuffix" -> expectedNagPaoStartSuffix,
    "paoEndNumber" -> expectedNagPaoEndNumber,
    "paoEndSuffix" -> expectedNagPaoEndSuffix,
    "saoText" -> expectedNagSaoText,
    "saoStartNumber" -> expectedNagSaoStartNumber,
    "saoStartSuffix" -> expectedNagSaoStartSuffix,
    "saoEndNumber" -> expectedNagSaoEndNumber,
    "saoEndSuffix" -> expectedNagSaoEndSuffix,
    "level" -> expectedNagLevel,
    "officialFlag" -> expectedNagOfficialFlag,
    "lpiLogicalStatus" -> expectedNagLpiLogicalStatus,
    "usrnMatchIndicator" -> expectedNagUsrnMatchIndicator,
    "language" -> expectedNagLanguage,
    "streetDescriptor" -> expectedNagStreetDescriptor,
    "townName" -> expectedNagTownName,
    "locality" -> expectedNagLocality,
    "streetClassification" -> expectedNagStreetClassification,
    "nagAll" -> expectedNagAll,
    "lpiStartDate" -> expectedNagLpiStartDate,
    "lpiLastUpdateDate" -> expectedNagLpiLastUpdateDate,
    "lpiEndDate" -> expectedNagLpiEndDate,
    "mixedNag" -> expectedNagMixed,
    "mixedWelshNag" -> expectedWelshNagMixed,
    "mixedNagStart" -> expectedMixedNagStart,
    "mixedWelshNagStart" -> expectedMixedWelshNagStart,
    "secondarySort" -> expectedNagSecondarySort,
    "country" -> expectedNagCountry
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
      val actual = HybridAddressEsDocument.rowToLpi(row)

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
      val actual = HybridAddressEsDocument.rowToPaf(row)

      // Then
      actual shouldBe expectedPaf
    }

    "concatenate the required paf fields for English and Welsh" in {

      // Given
      val pafBuildingNumber = "1000"
      val pafDependentThoroughfare = "throughfare"
      val pafWelshDependentThoroughfare = "welsh1"
      val pafPostcode = "POSTCODE"
      val pafPoBoxNumber = "6"
      val pafDependentLocality = "STIXTON"
      val pafWelshDependentLocality = "welsh4"
      val pafBuildingName = "COTTAGE"
      val pafOrganisationName = "CIBO"
      val pafPostTown = "LONDON"
      val pafWelshPostTown = "welsh5"
      val pafDepartmentName = "department"
      val pafDoubleDependentLocality = "locality"
      val pafWelshDoubleDependentLocality = "welsh3"
      val pafSubBuildingName = "FLAT E"
      val pafThoroughfare = "SOME_STREET"
      val pafWelshThoroughfare = "welsh2"

      // When
      val result = HybridAddressEsDocument.concatPaf(pafPoBoxNumber, pafBuildingNumber, pafDependentThoroughfare,
        pafWelshDependentThoroughfare, pafThoroughfare, pafWelshThoroughfare, pafDepartmentName, pafOrganisationName,
        pafSubBuildingName, pafBuildingName, pafDoubleDependentLocality, pafWelshDoubleDependentLocality,
        pafDependentLocality, pafWelshDependentLocality, pafPostTown, pafWelshPostTown, pafPostcode)

      // Then
      result shouldBe "department CIBO FLAT E COTTAGE 6 1000 throughfare welsh1 SOME_STREET welsh2 locality welsh3 STIXTON welsh4 LONDON welsh5 POSTCODE"
    }

    "change uppercase address to mixed case" in {
      // Given
      val pafBuildingName = "HMP WHITELEY"

      // When
      val result = HybridAddressEsDocument.generateFormattedPafAddress(expectedPaf("poBoxNumber").toString,
        expectedPaf("buildingNumber").toString, expectedPaf("dependentThoroughfare").toString,
        expectedPaf("thoroughfare").toString, expectedPaf("departmentName").toString ,
        expectedPaf("organisationName").toString, expectedPaf("subBuildingName").toString,
        pafBuildingName, expectedPaf("doubleDependentLocality").toString,
        expectedPaf("dependentLocality").toString, expectedPaf("postTown").toString,
        expectedPaf("postcode").toString)

      // Then
      result shouldBe "Department, Cibo, Flat E, HMP Whiteley, PO Box 6, 1 Throughfare, Some Street, Locality, Stixton, London, POSTCODE"
    }

    "change uppercase address containing number and character building name to mixed case" in {
      // Given
      val pafBuildingName = "50A"

      // When
      val result = HybridAddressEsDocument.generateFormattedPafAddress(expectedPaf("poBoxNumber").toString,
        expectedPaf("buildingNumber").toString, expectedPaf("dependentThoroughfare").toString,
        expectedPaf("thoroughfare").toString, expectedPaf("departmentName").toString ,
        expectedPaf("organisationName").toString, expectedPaf("subBuildingName").toString,
        pafBuildingName, expectedPaf("doubleDependentLocality").toString,
        expectedPaf("dependentLocality").toString, expectedPaf("postTown").toString,
        expectedPaf("postcode").toString)

      // Then
      result shouldBe "Department, Cibo, Flat E, 50A, PO Box 6, 1 Throughfare, Some Street, Locality, Stixton, London, POSTCODE"
    }

    "change uppercase address containing a hyphenated town name to mixed case" in {
      // Given
      val pafOrganisationName = "ACME STATS PLC"
      val pafDependentLocality = "LEE-ON-THE-SOLENT"
      val pafPostTown = "BARROW-IN-FURNESS"
      val pafBuildingName = "HMP WHITELEY"

      // When
      val result = HybridAddressEsDocument.generateFormattedPafAddress(expectedPaf("poBoxNumber").toString,
        expectedPaf("buildingNumber").toString, expectedPaf("dependentThoroughfare").toString,
        expectedPaf("thoroughfare").toString, expectedPaf("departmentName").toString ,
        pafOrganisationName, expectedPaf("subBuildingName").toString,
        pafBuildingName, expectedPaf("doubleDependentLocality").toString,
        pafDependentLocality, pafPostTown, expectedPaf("postcode").toString)

      // Then
      result shouldBe "Department, Acme Stats PLC, Flat E, HMP Whiteley, PO Box 6, 1 Throughfare, Some Street, Locality, Lee-on-the-Solent, Barrow-in-Furness, POSTCODE"
    }

    "avoid excess commas in buildings with a numbered building name, but no PO BOX or building number" in {
      // Given
      val pafBuildingName = "50A"
      val poBoxNumber = ""
      val buildingNumber = ""

      // When
      val result = HybridAddressEsDocument.generateFormattedPafAddress(
        poBoxNumber = poBoxNumber,
        buildingNumber = buildingNumber,
        dependentThoroughfare = expectedPaf("dependentThoroughfare").toString,
        thoroughfare = expectedPaf("thoroughfare").toString,
        departmentName = expectedPaf("departmentName").toString,
        organisationName = expectedPaf("organisationName").toString,
        subBuildingName = expectedPaf("subBuildingName").toString,
        buildingName = pafBuildingName,
        doubleDependentLocality = expectedPaf("doubleDependentLocality").toString,
        dependentLocality = expectedPaf("dependentLocality").toString,
        postTown = expectedPaf("postTown").toString,
        postcode = expectedPaf("postcode").toString
      )

      // Then
      result shouldBe "Department, Cibo, Flat E, 50A Throughfare, Some Street, Locality, Stixton, London, POSTCODE"
    }

    "keep excess commas in buildings with a worded building name, but no PO BOX or building number" in {
      // Given
      val pafBuildingName = "Flat 10"
      val poBoxNumber = ""
      val buildingNumber = ""

      // When
      val result = HybridAddressEsDocument.generateFormattedPafAddress(
        poBoxNumber = poBoxNumber,
        buildingNumber = buildingNumber,
        dependentThoroughfare = expectedPaf("dependentThoroughfare").toString,
        thoroughfare = expectedPaf("thoroughfare").toString,
        departmentName = expectedPaf("departmentName").toString,
        organisationName = expectedPaf("organisationName").toString,
        subBuildingName = expectedPaf("subBuildingName").toString,
        buildingName = pafBuildingName,
        doubleDependentLocality = expectedPaf("doubleDependentLocality").toString,
        dependentLocality = expectedPaf("dependentLocality").toString,
        postTown = expectedPaf("postTown").toString,
        postcode = expectedPaf("postcode").toString
      )

      // Then
      result shouldBe "Department, Cibo, Flat E, Flat 10, Throughfare, Some Street, Locality, Stixton, London, POSTCODE"
    }

    "change uppercase Welsh address to mixed case" in {
      // Given
      val pafBuildingName = "HMP NEWPORT"

      // When
      val result = HybridAddressEsDocument.generateWelshFormattedPafAddress(expectedPaf("poBoxNumber").toString,
        expectedPaf("buildingNumber").toString, expectedPaf("welshDependentThoroughfare").toString,
        expectedPaf("welshThoroughfare").toString, expectedPaf("departmentName").toString ,
        expectedPaf("organisationName").toString, expectedPaf("subBuildingName").toString,
        pafBuildingName, expectedPaf("welshDoubleDependentLocality").toString,
        expectedPaf("welshDependentLocality").toString, expectedPaf("welshPostTown").toString,
        expectedPaf("postcode").toString)

      // Then
      result shouldBe "Department, Cibo, Flat E, HMP Newport, PO Box 6, 1 Welsh1, Welsh2, Welsh3, Welsh4, Welsh5, POSTCODE"
    }

    "change uppercase Welsh address containing number and character building name to mixed case" in {
      // Given
      val pafBuildingName = "500A"

      // When
      val result = HybridAddressEsDocument.generateWelshFormattedPafAddress(expectedPaf("poBoxNumber").toString,
        expectedPaf("buildingNumber").toString, expectedPaf("welshDependentThoroughfare").toString,
        expectedPaf("welshThoroughfare").toString, expectedPaf("departmentName").toString ,
        expectedPaf("organisationName").toString, expectedPaf("subBuildingName").toString,
        pafBuildingName, expectedPaf("welshDoubleDependentLocality").toString,
        expectedPaf("welshDependentLocality").toString, expectedPaf("welshPostTown").toString,
        expectedPaf("postcode").toString)

      // Then
      result shouldBe "Department, Cibo, Flat E, 500A, PO Box 6, 1 Welsh1, Welsh2, Welsh3, Welsh4, Welsh5, POSTCODE"
    }

    "change uppercase nag address to mixed case" in {
      // Given
      val nagOrganisation = "ACME STATS PLC"

      // When
      val result = HybridAddressEsDocument.generateFormattedNagAddress(expectedNag("saoStartNumber").toString,
        expectedNag("saoStartSuffix").toString, expectedNag("saoEndNumber").toString,
        expectedNag("saoEndSuffix").toString, expectedNag("saoText").toString ,
        nagOrganisation, expectedNag("paoStartNumber").toString,
        expectedNag("paoStartSuffix").toString, expectedNag("paoEndNumber").toString,
        expectedNag("paoEndSuffix").toString, expectedNag("paoText").toString,
        expectedNag("streetDescriptor").toString, expectedNag("locality").toString,
        expectedNag("townName").toString, expectedNag("postcodeLocator").toString)

      // Then
      result shouldBe "Acme Stats PLC, 6473FF-6623JJ, The Building Name, A Training Centre, 56HH-7755OP And Another Street Descriptor, Locality Xyz, Town B, KL8 7HQ"
    }

    "change uppercase nag address containing 'PO BOX' to mixed case" in {
      // Given
      val nagOrganisation = "ACME STATS PLC"
      val saoText = "PO BOX 5678"

      // When
      val result = HybridAddressEsDocument.generateFormattedNagAddress(expectedNag("saoStartNumber").toString,
        expectedNag("saoStartSuffix").toString, expectedNag("saoEndNumber").toString,
        expectedNag("saoEndSuffix").toString, saoText,
        nagOrganisation, expectedNag("paoStartNumber").toString,
        expectedNag("paoStartSuffix").toString, expectedNag("paoEndNumber").toString,
        expectedNag("paoEndSuffix").toString, expectedNag("paoText").toString,
        expectedNag("streetDescriptor").toString, expectedNag("locality").toString,
        expectedNag("townName").toString, expectedNag("postcodeLocator").toString)

      // Then
      result shouldBe "Acme Stats PLC, 6473FF-6623JJ, PO Box 5678, A Training Centre, 56HH-7755OP And Another Street Descriptor, Locality Xyz, Town B, KL8 7HQ"
    }

    "pad the secondary sort field (for postcode search) with leading zeros where needed" in {
      // Given
      val secondarySort = "FLAT 43A AARVARKS R US UNIT 75"

      // When
      val result = HybridAddressEsDocument.addLeadingZeros(secondarySort)

      // Then
      result shouldBe "FLAT 0043A AARVARKS R US UNIT 0075"
    }

    "change uppercase nag address containing a hyphenated town name to mixed case" in {
      // Given
      val nagOrganisation = "ACME STATS PLC"
      val nagLocality = "LEE-ON-THE-SOLENT"
      val nagTown = "BARROW-IN-FURNESS"

      // When
      val result = HybridAddressEsDocument.generateFormattedNagAddress(expectedNag("saoStartNumber").toString,
        expectedNag("saoStartSuffix").toString, expectedNag("saoEndNumber").toString,
        expectedNag("saoEndSuffix").toString, expectedNag("saoText").toString ,
        nagOrganisation, expectedNag("paoStartNumber").toString,
        expectedNag("paoStartSuffix").toString, expectedNag("paoEndNumber").toString,
        expectedNag("paoEndSuffix").toString, expectedNag("paoText").toString,
        expectedNag("streetDescriptor").toString, nagLocality,
        nagTown, expectedNag("postcodeLocator").toString)

      // Then
      result shouldBe "Acme Stats PLC, 6473FF-6623JJ, The Building Name, A Training Centre, 56HH-7755OP And Another Street Descriptor, Lee-on-the-Solent, Barrow-in-Furness, KL8 7HQ"
    }

    "change uppercase nag address containing a hyphenated street descriptor to mixed case" in {
      // Given
      val nagOrganisation = "ACME STATS PLC"
      val streetDescriptor = "LEE-ON-THE-SOLENT PROMENADE WEST"

      // When
      val result = HybridAddressEsDocument.generateFormattedNagAddress(expectedNag("saoStartNumber").toString,
        expectedNag("saoStartSuffix").toString, expectedNag("saoEndNumber").toString,
        expectedNag("saoEndSuffix").toString, expectedNag("saoText").toString ,
        nagOrganisation, expectedNag("paoStartNumber").toString,
        expectedNag("paoStartSuffix").toString, expectedNag("paoEndNumber").toString,
        expectedNag("paoEndSuffix").toString, expectedNag("paoText").toString,
        streetDescriptor, expectedNag("locality").toString,
        expectedNag("townName").toString, expectedNag("postcodeLocator").toString)

      // Then
      result shouldBe "Acme Stats PLC, 6473FF-6623JJ, The Building Name, A Training Centre, 56HH-7755OP Lee-on-the-Solent Promenade West, Locality Xyz, Town B, KL8 7HQ"
    }

    "create NAG with expected formatted address (sao empty)" in {
      // Given
      val saoStartNumber = ""
      val saoStartSuffix  = ""
      val saoEndNumber = ""
      val saoEndSuffix = ""
      val saoText = ""

      // When
      val result = HybridAddressEsDocument.generateFormattedNagAddress(saoStartNumber,
        saoStartSuffix, saoEndNumber, saoEndSuffix, saoText, expectedNagOrganisation,
        expectedNag("paoStartNumber").toString, expectedNag("paoStartSuffix").toString,
        expectedNag("paoEndNumber").toString, expectedNag("paoEndSuffix").toString,
        expectedNag("paoText").toString, expectedNag("streetDescriptor").toString,
        expectedNag("locality").toString, expectedNag("townName").toString,
        expectedNag("postcodeLocator").toString)

      val expected = "Something Else, A Training Centre, 56HH-7755OP And Another Street Descriptor, Locality Xyz, Town B, KL8 7HQ"

      // Then
      result shouldBe expected
    }

    "create NAG with expected formatted address (pao empty)" in {
      // Given
      val paoStartNumber = ""
      val paoStartSuffix  = ""
      val paoEndNumber = ""
      val paoEndSuffix = ""
      val paoText = ""
      val saoText = "PO BOX 5678"

      // When
      val result = HybridAddressEsDocument.generateFormattedNagAddress(expectedNag("saoStartNumber").toString,
        expectedNag("saoStartSuffix").toString, expectedNag("saoEndNumber").toString,
        expectedNag("saoEndSuffix").toString, saoText, expectedNagOrganisation, paoStartNumber,
        paoStartSuffix, paoEndNumber, paoEndSuffix, paoText,
        expectedNag("streetDescriptor").toString, expectedNag("locality").toString,
        expectedNag("townName").toString, expectedNag("postcodeLocator").toString)

      val expected = "Something Else, 6473FF-6623JJ, PO Box 5678, And Another Street Descriptor, Locality Xyz, Town B, KL8 7HQ"

      // Then
      result shouldBe expected
    }

    "create NAG with expected formatted address (saoText field)" in {
      // Given
      val paoStartNumber = ""
      val saoText = "UNIT"
      val saoStartNumber = ""
      val saoStartSuffix  = ""
      val saoEndNumber = ""
      val saoEndSuffix = ""
      val paoStartSuffix  = ""
      val paoEndNumber = ""
      val paoEndSuffix = ""
      val paoText = ""

      // When
      val result = HybridAddressEsDocument.generateFormattedNagAddress(saoStartNumber,
        saoStartSuffix, saoEndNumber, saoEndSuffix, saoText, expectedNagOrganisation,
        paoStartNumber, paoStartSuffix,
        paoEndNumber, paoEndSuffix,
        paoText, expectedNag("streetDescriptor").toString,
        expectedNag("locality").toString, expectedNag("townName").toString,
        expectedNag("postcodeLocator").toString)

      val expected = "Something Else, Unit, And Another Street Descriptor, Locality Xyz, Town B, KL8 7HQ"

      // Then
      result shouldBe expected
    }

    "create NAG with expected formatted address (paoText field)" in {
      // Given
      val paoStartNumber = ""
      val saoText = ""
      val saoStartNumber = ""
      val saoStartSuffix  = ""
      val saoEndNumber = ""
      val saoEndSuffix = ""
      val paoStartSuffix  = ""
      val paoEndNumber = ""
      val paoEndSuffix = ""
      val paoText = "UNIT"

      // When
      val result = HybridAddressEsDocument.generateFormattedNagAddress(saoStartNumber,
        saoStartSuffix, saoEndNumber, saoEndSuffix, saoText, expectedNagOrganisation,
        paoStartNumber, paoStartSuffix,
        paoEndNumber, paoEndSuffix,
        paoText, expectedNag("streetDescriptor").toString,
        expectedNag("locality").toString, expectedNag("townName").toString,
        expectedNag("postcodeLocator").toString)

      val expected = "Something Else, Unit, And Another Street Descriptor, Locality Xyz, Town B, KL8 7HQ"

      // Then
      result shouldBe expected
    }

    "create NAG with expected formatted address (saoText and paoText fields)" in {
      // Given
      val paoStartNumber = ""
      val saoText = "UNIT"
      val saoStartNumber = ""
      val saoStartSuffix  = ""
      val saoEndNumber = ""
      val saoEndSuffix = ""
      val paoStartSuffix  = ""
      val paoEndNumber = ""
      val paoEndSuffix = ""
      val paoText = "BUNIT"

      // When
      val result = HybridAddressEsDocument.generateFormattedNagAddress(saoStartNumber,
        saoStartSuffix, saoEndNumber, saoEndSuffix, saoText, expectedNagOrganisation,
        paoStartNumber, paoStartSuffix,
        paoEndNumber, paoEndSuffix,
        paoText, expectedNag("streetDescriptor").toString,
        expectedNag("locality").toString, expectedNag("townName").toString,
        expectedNag("postcodeLocator").toString)

      val expected = "Something Else, Unit, Bunit, And Another Street Descriptor, Locality Xyz, Town B, KL8 7HQ"

      // Then
      result shouldBe expected
    }

    "concatenate the required paf fields and handle empty strings" in {

      // Given
      val pafBuildingNumber = ""
      val pafDependentThoroughfare = ""
      val pafWelshDependentThoroughfare = ""
      val pafPostcode = "POSTCODE"
      val pafPoBoxNumber = ""
      val pafDependentLocality = ""
      val pafWelshDependentLocality = ""
      val pafBuildingName = ""
      val pafOrganisationName = ""
      val pafPostTown = "LONDON"
      val pafWelshPostTown = ""
      val pafDepartmentName = ""
      val pafDoubleDependentLocality = ""
      val pafWelshDoubleDependentLocality = ""
      val pafSubBuildingName = ""
      val pafThoroughfare = ""
      val pafWelshThoroughfare = ""

      // When
      val result = HybridAddressEsDocument.concatPaf(pafPoBoxNumber, pafBuildingNumber, pafDependentThoroughfare,
        pafWelshDependentThoroughfare, pafThoroughfare, pafWelshThoroughfare, pafDepartmentName, pafOrganisationName,
        pafSubBuildingName, pafBuildingName, pafDoubleDependentLocality, pafWelshDoubleDependentLocality,
        pafDependentLocality, pafWelshDependentLocality, pafPostTown, pafWelshPostTown, pafPostcode)

      // Then
      result shouldBe "LONDON POSTCODE"
    }

    "concatenate the required paf fields for English and Welsh with missing Welsh values" in {

      // Given
      val pafBuildingNumber = "1000"
      val pafDependentThoroughfare = "throughfare"
      val pafWelshDependentThoroughfare = "welsh1"
      val pafPostcode = "POSTCODE"
      val pafPoBoxNumber = "6"
      val pafDependentLocality = "STIXTON"
      val pafWelshDependentLocality = ""
      val pafBuildingName = "COTTAGE"
      val pafOrganisationName = "CIBO"
      val pafPostTown = "LONDON"
      val pafWelshPostTown = "welsh5"
      val pafDepartmentName = "department"
      val pafDoubleDependentLocality = "locality"
      val pafWelshDoubleDependentLocality = ""
      val pafSubBuildingName = "FLAT E"
      val pafThoroughfare = "SOME_STREET"
      val pafWelshThoroughfare = "welsh2"

      // When
      val result = HybridAddressEsDocument.concatPaf(pafPoBoxNumber, pafBuildingNumber, pafDependentThoroughfare,
        pafWelshDependentThoroughfare, pafThoroughfare, pafWelshThoroughfare, pafDepartmentName, pafOrganisationName,
        pafSubBuildingName, pafBuildingName, pafDoubleDependentLocality, pafWelshDoubleDependentLocality,
        pafDependentLocality, pafWelshDependentLocality, pafPostTown, pafWelshPostTown, pafPostcode)

      // Then
      result shouldBe "department CIBO FLAT E COTTAGE 6 1000 throughfare welsh1 SOME_STREET welsh2 locality STIXTON LONDON welsh5 POSTCODE"
    }

    "concatenate the required paf fields for English and Welsh with matching Welsh values" in {

      // Given
      val pafBuildingNumber = "1000"
      val pafDependentThoroughfare = "throughfare"
      val pafWelshDependentThoroughfare = "throughfare"
      val pafPostcode = "POSTCODE"
      val pafPoBoxNumber = "6"
      val pafDependentLocality = "STIXTON"
      val pafWelshDependentLocality = "STIXTON"
      val pafBuildingName = "COTTAGE"
      val pafOrganisationName = "CIBO"
      val pafPostTown = "LONDON"
      val pafWelshPostTown = "LONDON"
      val pafDepartmentName = "department"
      val pafDoubleDependentLocality = "locality"
      val pafWelshDoubleDependentLocality = "locality"
      val pafSubBuildingName = "FLAT E"
      val pafThoroughfare = "SOME_STREET"
      val pafWelshThoroughfare = "SOME_STREET"

      // When
      val result = HybridAddressEsDocument.concatPaf(pafPoBoxNumber, pafBuildingNumber, pafDependentThoroughfare,
        pafWelshDependentThoroughfare, pafThoroughfare, pafWelshThoroughfare, pafDepartmentName, pafOrganisationName,
        pafSubBuildingName, pafBuildingName, pafDoubleDependentLocality, pafWelshDoubleDependentLocality,
        pafDependentLocality, pafWelshDependentLocality, pafPostTown, pafWelshPostTown, pafPostcode)

      // Then
      result shouldBe "department CIBO FLAT E COTTAGE 6 1000 throughfare SOME_STREET locality STIXTON LONDON POSTCODE"
    }

    "concatenate the required nag fields" in {

      // Given
      val nagOrganisation = "SOMETHING ELSE"
      val nagPaoStartNumber = "56"
      val nagPostcodeLocator = "KL8 7HQ"
      val nagSaoEndSuffix = "JJ"
      val nagSaoStartNumber = "6473"
      val nagPaoEndSuffix = "OP"
      val nagStreetDescriptor = "AND ANOTHER STREET DESCRIPTOR"
      val nagSaoEndNumber = "6623"
      val nagPaoEndNumber = "7755"
      val nagTownName = "TOWN B"
      val nagSaoStartSuffix = "FF"
      val nagPaoText = "A TRAINING CENTRE"
      val nagSaoText = "THE BUILDING NAME"
      val nagPaoStartSuffix = "HH"
      val nagLocality = "LOCALITY XYZ"

      // When
      val result = HybridAddressEsDocument.concatNag(nagSaoStartNumber, nagSaoEndNumber, nagSaoEndSuffix, nagSaoStartSuffix,
        nagSaoText, nagOrganisation, nagPaoStartNumber, nagPaoStartSuffix, nagPaoEndNumber, nagPaoEndSuffix,
        nagPaoText, nagStreetDescriptor, nagTownName, nagLocality, nagPostcodeLocator)

      // Then
      result shouldBe "SOMETHING ELSE 6473FF-6623JJ THE BUILDING NAME A TRAINING CENTRE 56HH-7755OP AND ANOTHER STREET DESCRIPTOR LOCALITY XYZ TOWN B KL8 7HQ"
    }

    "concatenate the required nag fields and handle empty start numbers" in {

      // Given
      val nagOrganisation = "SOMETHING ELSE"
      val nagPaoStartNumber = ""
      val nagPostcodeLocator = "KL8 7HQ"
      val nagSaoEndSuffix = "JJ"
      val nagSaoStartNumber = ""
      val nagPaoEndSuffix = "OP"
      val nagStreetDescriptor = "AND ANOTHER STREET DESCRIPTOR"
      val nagSaoEndNumber = "6623"
      val nagPaoEndNumber = "7755"
      val nagTownName = "TOWN B"
      val nagSaoStartSuffix = "FF"
      val nagPaoText = "A TRAINING CENTRE"
      val nagSaoText = "THE BUILDING NAME"
      val nagPaoStartSuffix = "HH"
      val nagLocality = "LOCALITY XYZ"

      // When
      val result = HybridAddressEsDocument.concatNag(nagSaoStartNumber, nagSaoEndNumber, nagSaoEndSuffix, nagSaoStartSuffix,
        nagSaoText, nagOrganisation, nagPaoStartNumber, nagPaoStartSuffix, nagPaoEndNumber, nagPaoEndSuffix,
        nagPaoText, nagStreetDescriptor, nagTownName, nagLocality, nagPostcodeLocator)

      // Then
      result shouldBe "SOMETHING ELSE FF-6623JJ THE BUILDING NAME A TRAINING CENTRE HH-7755OP AND ANOTHER STREET DESCRIPTOR LOCALITY XYZ TOWN B KL8 7HQ"
    }

    "concatenate the required nag fields and handle empty sao details" in {

      // Given
      val nagOrganisation = "SOMETHING ELSE"
      val nagPaoStartNumber = "56"
      val nagPostcodeLocator = "KL8 7HQ"
      val nagSaoEndSuffix = "JJ"
      val nagSaoStartNumber = ""
      val nagPaoEndSuffix = "OP"
      val nagStreetDescriptor = "AND ANOTHER STREET DESCRIPTOR"
      val nagSaoEndNumber = "6623"
      val nagPaoEndNumber = "7755"
      val nagTownName = "TOWN B"
      val nagSaoStartSuffix = ""
      val nagPaoText = "A TRAINING CENTRE"
      val nagSaoText = "THE BUILDING NAME"
      val nagPaoStartSuffix = "HH"
      val nagLocality = "LOCALITY XYZ"

      // When
      val result = HybridAddressEsDocument.concatNag(nagSaoStartNumber, nagSaoEndNumber, nagSaoEndSuffix, nagSaoStartSuffix,
        nagSaoText, nagOrganisation, nagPaoStartNumber, nagPaoStartSuffix, nagPaoEndNumber, nagPaoEndSuffix,
        nagPaoText, nagStreetDescriptor, nagTownName, nagLocality, nagPostcodeLocator)

      // Then
      result shouldBe "SOMETHING ELSE 6623JJ THE BUILDING NAME A TRAINING CENTRE 56HH-7755OP AND ANOTHER STREET DESCRIPTOR LOCALITY XYZ TOWN B KL8 7HQ"
    }

    "concatenate the required nag fields and handle empty end numbers" in {

      // Given
      val nagOrganisation = "SOMETHING ELSE"
      val nagPaoStartNumber = "56"
      val nagPostcodeLocator = "KL8 7HQ"
      val nagSaoEndSuffix = "JJ"
      val nagSaoStartNumber = "6473"
      val nagPaoEndSuffix = "OP"
      val nagStreetDescriptor = "AND ANOTHER STREET DESCRIPTOR"
      val nagSaoEndNumber = ""
      val nagPaoEndNumber = ""
      val nagTownName = "TOWN B"
      val nagSaoStartSuffix = "FF"
      val nagPaoText = "A TRAINING CENTRE"
      val nagSaoText = "THE BUILDING NAME"
      val nagPaoStartSuffix = "HH"
      val nagLocality = "LOCALITY XYZ"

      // When
      val result = HybridAddressEsDocument.concatNag(nagSaoStartNumber, nagSaoEndNumber, nagSaoEndSuffix, nagSaoStartSuffix,
        nagSaoText, nagOrganisation, nagPaoStartNumber, nagPaoStartSuffix, nagPaoEndNumber, nagPaoEndSuffix,
        nagPaoText, nagStreetDescriptor, nagTownName, nagLocality, nagPostcodeLocator)

      // Then
      result shouldBe "SOMETHING ELSE 6473FF-JJ THE BUILDING NAME A TRAINING CENTRE 56HH-OP AND ANOTHER STREET DESCRIPTOR LOCALITY XYZ TOWN B KL8 7HQ"
    }

    "concatenate the required nag fields and handle empty end pao details" in {

      // Given
      val nagOrganisation = "SOMETHING ELSE"
      val nagPaoStartNumber = "56"
      val nagPostcodeLocator = "KL8 7HQ"
      val nagSaoEndSuffix = ""
      val nagSaoStartNumber = "6473"
      val nagPaoEndSuffix = ""
      val nagStreetDescriptor = "AND ANOTHER STREET DESCRIPTOR"
      val nagSaoEndNumber = ""
      val nagPaoEndNumber = ""
      val nagTownName = "TOWN B"
      val nagSaoStartSuffix = "FF"
      val nagPaoText = "A TRAINING CENTRE"
      val nagSaoText = "THE BUILDING NAME"
      val nagPaoStartSuffix = "HH"
      val nagLocality = "LOCALITY XYZ"

      // When
      val result = HybridAddressEsDocument.concatNag(nagSaoStartNumber, nagSaoEndNumber, nagSaoEndSuffix, nagSaoStartSuffix,
        nagSaoText, nagOrganisation, nagPaoStartNumber, nagPaoStartSuffix, nagPaoEndNumber, nagPaoEndSuffix,
        nagPaoText, nagStreetDescriptor, nagTownName, nagLocality, nagPostcodeLocator)

      // Then
      result shouldBe "SOMETHING ELSE 6473FF THE BUILDING NAME A TRAINING CENTRE 56HH AND ANOTHER STREET DESCRIPTOR LOCALITY XYZ TOWN B KL8 7HQ"
    }

    "concatenate the required nag fields and handle empty pao details" in {

      // Given
      val nagOrganisation = "SOMETHING ELSE"
      val nagPaoStartNumber = ""
      val nagPostcodeLocator = "KL8 7HQ"
      val nagSaoEndSuffix = "JJ"
      val nagSaoStartNumber = "6473"
      val nagPaoEndSuffix = ""
      val nagStreetDescriptor = "AND ANOTHER STREET DESCRIPTOR"
      val nagSaoEndNumber = "6623"
      val nagPaoEndNumber = ""
      val nagTownName = "TOWN B"
      val nagSaoStartSuffix = "FF"
      val nagPaoText = "A TRAINING CENTRE"
      val nagSaoText = "THE BUILDING NAME"
      val nagPaoStartSuffix = ""
      val nagLocality = "LOCALITY XYZ"

      // When
      val result = HybridAddressEsDocument.concatNag(nagSaoStartNumber, nagSaoEndNumber, nagSaoEndSuffix, nagSaoStartSuffix,
        nagSaoText, nagOrganisation, nagPaoStartNumber, nagPaoStartSuffix, nagPaoEndNumber, nagPaoEndSuffix,
        nagPaoText, nagStreetDescriptor, nagTownName, nagLocality, nagPostcodeLocator)

      // Then
      result shouldBe "SOMETHING ELSE 6473FF-6623JJ THE BUILDING NAME A TRAINING CENTRE AND ANOTHER STREET DESCRIPTOR LOCALITY XYZ TOWN B KL8 7HQ"
    }

    "concatenate the required nag fields and handle empty strings" in {

      // Given
      val nagOrganisation = ""
      val nagPaoStartNumber = "56"
      val nagPostcodeLocator = "KL8 7HQ"
      val nagSaoEndSuffix = "JJ"
      val nagSaoStartNumber = "6473"
      val nagPaoEndSuffix = ""
      val nagStreetDescriptor = "AND ANOTHER STREET DESCRIPTOR"
      val nagSaoEndNumber = "6623"
      val nagPaoEndNumber = "7755"
      val nagTownName = ""
      val nagSaoStartSuffix = ""
      val nagPaoText = ""
      val nagSaoText = "THE BUILDING NAME"
      val nagPaoStartSuffix = "HH"
      val nagLocality = ""

      // When
      val result = HybridAddressEsDocument.concatNag(nagSaoStartNumber, nagSaoEndNumber, nagSaoEndSuffix, nagSaoStartSuffix,
        nagSaoText, nagOrganisation, nagPaoStartNumber, nagPaoStartSuffix, nagPaoEndNumber, nagPaoEndSuffix,
        nagPaoText, nagStreetDescriptor, nagTownName, nagLocality, nagPostcodeLocator)

      // Then
      result shouldBe "6473-6623JJ THE BUILDING NAME 56HH-7755 AND ANOTHER STREET DESCRIPTOR KL8 7HQ"
    }

    "generate concatenated paf (all fields) with commas as recommended by Royal Mail" in {

      // Given
      val pafBuildingNumber = "1000"
      val pafDependentThoroughfare = "throughfare"
      val pafPostcode = "POSTCODE"
      val pafPoBoxNumber = "6"
      val pafDependentLocality = "STIXTON"
      val pafBuildingName = "COTTAGE"
      val pafOrganisationName = "CIBO"
      val pafPostTown = "LONDON"
      val pafDepartmentName = "department"
      val pafDoubleDependentLocality = "locality"
      val pafSubBuildingName = "FLAT E"
      val pafThoroughfare = "SOME_STREET"

      // When
      val result = HybridAddressEsDocument.generatePaf(
        poBoxNumber= pafPoBoxNumber, buildingNumber = pafBuildingNumber,
        dependentThoroughfare = pafDependentThoroughfare, thoroughfare= pafThoroughfare,
        departmentName = pafDepartmentName, organisationName = pafOrganisationName,
        subBuildingName = pafSubBuildingName, buildingName = pafBuildingName,
        doubleDependentLocality = pafDoubleDependentLocality, dependentLocality = pafDependentLocality,
        postTown = pafPostTown, postcode = pafPostcode
      )

      // Then
      result shouldBe List("Department", "Cibo", "Flat E", "Cottage", "PO Box 6", "1000 Throughfare", "Some_street", "Locality", "Stixton", "London", "POSTCODE")

    }

    "generate concatenated paf (building number no dependent locality) with commas as recommended by Royal Mail" in {

      // Given
      val pafBuildingNumber = "1000"
      val pafDependentThoroughfare = ""
      val pafPostcode = "POSTCODE"
      val pafPoBoxNumber = ""
      val pafDependentLocality = "SIXTON"
      val pafBuildingName = ""
      val pafOrganisationName = ""
      val pafPostTown = "LONDON"
      val pafDepartmentName = ""
      val pafDoubleDependentLocality = ""
      val pafSubBuildingName = ""
      val pafThoroughfare = "SOME_STREET"

      // When
      val result = HybridAddressEsDocument.generatePaf(
        poBoxNumber= pafPoBoxNumber, buildingNumber = pafBuildingNumber,
        dependentThoroughfare = pafDependentThoroughfare, thoroughfare= pafThoroughfare,
        departmentName = pafDepartmentName, organisationName = pafOrganisationName,
        subBuildingName = pafSubBuildingName, buildingName = pafBuildingName,
        doubleDependentLocality = pafDoubleDependentLocality, dependentLocality = pafDependentLocality,
        postTown = pafPostTown, postcode = pafPostcode
      )

      // Then
      result shouldBe List("1000 Some_street", "Sixton", "London", "POSTCODE")

    }

    "generate concatenated paf (building name containing number no dependent locality) with commas as recommended by Royal Mail" in {

      // Given
      val pafBuildingNumber = ""
      val pafDependentThoroughfare = ""
      val pafPostcode = "POSTCODE"
      val pafPoBoxNumber = ""
      val pafDependentLocality = "SIXTON"
      val pafBuildingName = "22B"
      val pafOrganisationName = ""
      val pafPostTown = "LONDON"
      val pafDepartmentName = ""
      val pafDoubleDependentLocality = ""
      val pafSubBuildingName = ""
      val pafThoroughfare = "BAKER STREET"

      // When
      val result = HybridAddressEsDocument.generatePaf(
        poBoxNumber= pafPoBoxNumber, buildingNumber = pafBuildingNumber,
        dependentThoroughfare = pafDependentThoroughfare, thoroughfare= pafThoroughfare,
        departmentName = pafDepartmentName, organisationName = pafOrganisationName,
        subBuildingName = pafSubBuildingName, buildingName = pafBuildingName,
        doubleDependentLocality = pafDoubleDependentLocality, dependentLocality = pafDependentLocality,
        postTown = pafPostTown, postcode = pafPostcode
      )

      // Then
      result shouldBe List("22B Baker Street", "Sixton", "London", "POSTCODE")

    }

    "capitalize first character of string" in {
      // Given
      val inputString = "the Oaks"
      val outputString = "The Oaks"

      //When
      val result = HybridAddressEsDocument.capitalizeFirst(inputString)

      // Then
      result shouldBe outputString
    }

  }
}
