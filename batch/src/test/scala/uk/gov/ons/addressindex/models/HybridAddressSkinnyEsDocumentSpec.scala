package uk.gov.ons.addressindex.models

import org.apache.spark.sql.Row
import org.scalatest.{Matchers, WordSpec}

class HybridAddressSkinnyEsDocumentSpec extends WordSpec with Matchers {

  val format = new java.text.SimpleDateFormat("yyyy-MM-dd")

  val pafBuildingNumber = 1.toShort
  val pafUdprn = 19
  val pafLastUpdateDate = new java.sql.Date(format.parse("2016-02-10").getTime)
  val pafProOrder = 272650L
  val pafEndDate = new java.sql.Date(format.parse("2012-04-25").getTime)
  val pafPostcodeType = "S"
  val pafDependentThoroughfare = "throughfare"
  val pafEntryDate = new java.sql.Date(format.parse("2012-03-19").getTime)
  val pafWelshPostTown = "welsh5"
  val pafDeliveryPointSuffix = "1Q"
  val pafPostcode = "POSTCODE"
  val pafProcessDate = new java.sql.Date(format.parse("2016-01-18").getTime)
  val pafPoBoxNumber = "6"
  val pafUprn = 1L
  val pafDependentLocality = "STIXTON"
  val pafBuildingName = "COTTAGE"
  val pafWelshDoubleDependentLocality = "welsh3"
  val pafOrganisationName = "CIBO"
  val pafPostTown = "LONDON"
  val pafChangeType = "I"
  val pafDepartmentName = "department"
  val pafWelshDependentLocality = "welsh4"
  val pafDoubleDependentLocality = "locality"
  val pafWelshDependentThoroughfare = "welsh1"
  val pafSubBuildingName = "FLAT E"
  val pafWelshThoroughfare = "welsh2"
  val pafThoroughfare = "SOME_STREET"
  val pafStartDate = new java.sql.Date(format.parse("2012-04-23").getTime)
  val pafRecordIdentifier = 27.toByte
  val pafAll = "department CIBO FLAT E COTTAGE 6 1 throughfare welsh1 SOME_STREET welsh2 locality welsh3 STIXTON welsh4 LONDON welsh5 POSTCODE"
  val pafMixed = "Department, Cibo, Flat E, Cottage, PO BOX 6, 1 Throughfare, Some_street, Locality, Stixton, London, POSTCODE"
  val pafWelshMixed = "Department, Cibo, Flat E, Cottage, PO BOX 6, 1 Welsh1, Welsh2, Welsh3, Welsh4, Welsh5, POSTCODE"

  val nagOrganisation = "SOMETHING ELSE"
  val nagOfficialFlag = "Y"
  val nagPaoStartNumber = 56.toShort
  val nagPostcodeLocator = "KL8 7HQ"
  val nagSaoEndSuffix = "JJ"
  val nagSaoStartNumber = 6473.toShort
  val nagUsrn = 9402538
  val nagLpiLogicalStatus = 1.toByte
  val nagEasting = 379171.00F
  val nagPaoEndSuffix = "OP"
  val nagStreetDescriptor = "AND ANOTHER STREET DESCRIPTOR"
  val nagUprn = 100010971565L
  val nagNorthing = 412816.00F
  val nagLpiKey = "1610L000014429"
  val nagSaoEndNumber = 6623.toShort
  val nagPaoEndNumber = 7755.toShort
  val nagTownName = "TOWN B"
  val nagLegalName = "ANOTHER LEGAL NAME"
  val nagSaoStartSuffix = "FF"
  val nagPaoText = "A TRAINING CENTRE"
  val nagSaoText = "THE BUILDING NAME"
  val nagPaoStartSuffix = "HH"
  val nagAddressBasePostal = "D"
  val nagLocality = "LOCALITY XYZ"
  val nagLevel = "UP THERE SOME WHERE"
  val nagParentUprn = 999910971564L
  val nagMultiOccCount = 0.toShort
  val nagBlpuLogicalStatus = 1.toByte
  val nagLocalCustodianCode = 4218.toShort
  val nagRpc = 1.toByte
  val nagClassScheme = "AddressBase Premium Classification Scheme"
  val nagUsrnMatchIndicator = 1.toByte
  val nagLanguage = "ENG"
  val nagStreetClassification = 8.toByte
  val nagLocation = Array(-2.3162985F, 4.00F)
  val nagAll = "SOMETHING ELSE 6473FF-6623JJ THE BUILDING NAME A TRAINING CENTRE 56HH-7755OP AND ANOTHER STREET DESCRIPTOR LOCALITY XYZ TOWN B KL8 7HQ"
  val nagLpiStartDate = new java.sql.Date(format.parse("2012-04-23").getTime)
  val nagLpiLastUpdateDate = new java.sql.Date(format.parse("2012-04-24").getTime)
  val nagLpiEndDate = new java.sql.Date(format.parse("2018-01-11").getTime)
  val nagMixed = "Something Else, 6473FF-6623JJ, The Building Name, A Training Centre, 56HH-7755OP And Another Street Descriptor, Locality Xyz, Town B, KL8 7HQ"

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

  val expectedPaf = Map(
    "endDate" -> pafEndDate,
    "uprn" -> pafUprn,
    "startDate" -> pafStartDate,
    "pafAll" -> pafAll,
    "mixedPaf" -> pafMixed,
    "mixedWelshPaf" -> pafWelshMixed
  )

  val expectedNag = Map(
    "uprn" -> nagUprn,
    "postcodeLocator" -> nagPostcodeLocator,
    "addressBasePostal" -> nagAddressBasePostal,
    "location" -> nagLocation,
    "easting" -> nagEasting,
    "northing" -> nagNorthing,
    "parentUprn" -> nagParentUprn,
    "paoStartNumber" -> nagPaoStartNumber,
    "paoStartSuffix" -> nagPaoStartSuffix,
    "saoStartNumber" -> nagSaoStartNumber,
    "lpiLogicalStatus" -> nagLpiLogicalStatus,
    "streetDescriptor" -> nagStreetDescriptor,
    "nagAll" -> nagAll,
    "lpiStartDate" -> nagLpiStartDate,
    "lpiEndDate" -> nagLpiEndDate,
    "mixedNag" -> nagMixed
  )

  val expectedNisra = Map(
    "uprn" -> nisraUprn,
    "location" -> nisraLocation,
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
        nagUprn,
        nagPostcodeLocator,
        nagAddressBasePostal,
        nagLocation,
        nagEasting,
        nagNorthing,
        nagParentUprn,
        nagMultiOccCount,
        nagBlpuLogicalStatus,
        nagLocalCustodianCode,
        nagRpc,
        nagOrganisation,
        nagLegalName,
        nagUsrn,
        nagLpiKey,
        nagPaoText,
        nagPaoStartNumber,
        nagPaoStartSuffix,
        nagPaoEndNumber,
        nagPaoEndSuffix,
        nagSaoText,
        nagSaoStartNumber,
        nagSaoStartSuffix,
        nagSaoEndNumber,
        nagSaoEndSuffix,
        nagLevel,
        nagOfficialFlag,
        nagLpiLogicalStatus,
        nagUsrnMatchIndicator,
        nagLanguage,
        nagStreetDescriptor,
        nagTownName,
        nagLocality,
        nagStreetClassification,
        nagLpiStartDate,
        nagLpiLastUpdateDate,
        nagLpiEndDate
      )

      // When
      val actual = HybridAddressSkinnyEsDocument.rowToLpi(row)

      // Then
      actual shouldBe expectedNag
    }

    "cast DataFrame's rows to an PAF key-value Map" in {
      // Given
      val row = Row(
        pafRecordIdentifier,
        pafChangeType,
        pafProOrder,
        pafUprn,
        pafUdprn,
        pafOrganisationName,
        pafDepartmentName,
        pafSubBuildingName,
        pafBuildingName,
        pafBuildingNumber,
        pafDependentThoroughfare,
        pafThoroughfare,
        pafDoubleDependentLocality,
        pafDependentLocality,
        pafPostTown,
        pafPostcode,
        pafPostcodeType,
        pafDeliveryPointSuffix,
        pafWelshDependentThoroughfare,
        pafWelshThoroughfare,
        pafWelshDoubleDependentLocality,
        pafWelshDependentLocality,
        pafWelshPostTown,
        pafPoBoxNumber,
        pafProcessDate,
        pafStartDate,
        pafEndDate,
        pafLastUpdateDate,
        pafEntryDate
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
        val actual = HybridAddressSkinnyEsDocument.rowToNisra(row)

        // Then
        actual shouldBe expectedNisra
      }
    }

    "create NISRA with expected formatted address" in {

      // Also tests nisraAll
      // When
      val result = HybridAddressSkinnyEsDocument.generateFormattedNisraAddresses(nisraOrganisation, nisraSubBuildingName,
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
      val result = HybridAddressSkinnyEsDocument.generateFormattedNisraAddresses(nisraOrganisation, nisraSubBuildingName,
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
