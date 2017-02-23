package uk.gov.ons.addressindex.models

import org.apache.spark.sql.Row
import org.scalatest.{Matchers, WordSpec}

class HybridAddressEsDocumentSpec extends WordSpec with Matchers {

  val format = new java.text.SimpleDateFormat("yyyy-MM-dd")

  "Hybrid Address Elastic Search Document" should {

    val nagOrganisation = "SOMETHING ELSE"
    val nagOfficialFlag = "Y"
    val nagClassificationCode = "RD"
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
    val nagCrossReference = "E04000324"
    val nagSource = "7666MI"
    val nagLocation = Array(-2.3162985F, 4.00F)
    val nagAll = "SOMETHING ELSE 6473FF-6623JJ THE BUILDING NAME A TRAINING CENTRE 56HH-7755OP AND ANOTHER STREET DESCRIPTOR LOCALITY XYZ TOWN B KL8 7HQ"


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
    val pafOrganizationName = "CIBO"
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
        nagClassScheme,
        nagClassificationCode,
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
        nagCrossReference,
        nagSource,
        nagAll
      )

      val expected = Map(
        "uprn" -> nagUprn,
        "postcodeLocator" -> nagPostcodeLocator,
        "addressBasePostal" -> nagAddressBasePostal,
        "location" -> nagLocation,
        "easting" -> nagEasting,
        "northing" -> nagNorthing,
        "parentUprn" -> nagParentUprn,
        "multiOccCount" -> nagMultiOccCount,
        "blpuLogicalStatus" -> nagBlpuLogicalStatus,
        "localCustodianCode" -> nagLocalCustodianCode,
        "rpc" -> nagRpc,
        "organisation" -> nagOrganisation,
        "legalName" -> nagLegalName,
        "classScheme" -> nagClassScheme,
        "classificationCode" -> nagClassificationCode,
        "usrn" -> nagUsrn,
        "lpiKey" -> nagLpiKey,
        "paoText" -> nagPaoText,
        "paoStartNumber" -> nagPaoStartNumber,
        "paoStartSuffix" -> nagPaoStartSuffix,
        "paoEndNumber" -> nagPaoEndNumber,
        "paoEndSuffix" -> nagPaoEndSuffix,
        "saoText" -> nagSaoText,
        "saoStartNumber" -> nagSaoStartNumber,
        "saoStartSuffix" -> nagSaoStartSuffix,
        "saoEndNumber" -> nagSaoEndNumber,
        "saoEndSuffix" -> nagSaoEndSuffix,
        "level" -> nagLevel,
        "officialFlag" -> nagOfficialFlag,
        "lpiLogicalStatus" -> nagLpiLogicalStatus,
        "usrnMatchIndicator" -> nagUsrnMatchIndicator,
        "language" -> nagLanguage,
        "streetDescriptor" -> nagStreetDescriptor,
        "townName" -> nagTownName,
        "locality" -> nagLocality,
        "streetClassification" -> nagStreetClassification,
        "crossReference" -> nagCrossReference,
        "source" -> nagSource,
        "nagAll" -> nagAll
      )

      // When
      val actual = HybridAddressEsDocument.rowToLpi(row)

      // Then
      actual shouldBe expected
    }

    "cast DataFrame's rows to an PAF key-value Map" in {
      // Given
      val row = Row(
        pafRecordIdentifier,
        pafChangeType,
        pafProOrder,
        pafUprn,
        pafUdprn,
        pafOrganizationName,
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
        pafEntryDate,
        pafAll
      )

      val expected = Map(
        "buildingNumber" -> pafBuildingNumber,
        "udprn" -> pafUdprn,
        "lastUpdateDate" -> pafLastUpdateDate,
        "proOrder" -> pafProOrder,
        "endDate" -> pafEndDate,
        "postcodeType" -> pafPostcodeType,
        "dependentThoroughfare" -> pafDependentThoroughfare,
        "entryDate" -> pafEntryDate,
        "welshPostTown" -> pafWelshPostTown,
        "deliveryPointSuffix" -> pafDeliveryPointSuffix,
        "postcode" -> pafPostcode,
        "processDate" -> pafProcessDate,
        "poBoxNumber" -> pafPoBoxNumber,
        "uprn" -> pafUprn,
        "dependentLocality" -> pafDependentLocality,
        "buildingName" -> pafBuildingName,
        "welshDoubleDependentLocality" -> pafWelshDoubleDependentLocality,
        "organisationName" -> pafOrganizationName,
        "postTown" -> pafPostTown,
        "changeType" -> pafChangeType,
        "departmentName" -> pafDepartmentName,
        "welshDependentLocality" -> pafWelshDependentLocality,
        "doubleDependentLocality" -> pafDoubleDependentLocality,
        "welshDependentThoroughfare" -> pafWelshDependentThoroughfare,
        "subBuildingName" -> pafSubBuildingName,
        "welshThoroughfare" -> pafWelshThoroughfare,
        "thoroughfare" -> pafThoroughfare,
        "startDate" -> pafStartDate,
        "recordIdentifier" -> pafRecordIdentifier,
        "pafAll" -> pafAll
      )

      // When
      val actual = HybridAddressEsDocument.rowToPaf(row)

      // Then
      actual shouldBe expected
    }

  }
}
