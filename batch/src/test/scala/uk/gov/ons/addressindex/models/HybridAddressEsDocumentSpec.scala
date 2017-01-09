package uk.gov.ons.addressindex.models

import org.apache.spark.sql.Row
import org.scalatest.{Matchers, WordSpec}

class HybridAddressEsDocumentSpec extends WordSpec with Matchers {
  "Hybrid Address Elastic Search Document" should {

    val nagOrganisation = "SOMETHING ELSE"
    val nagOfficialFlag = "Y"
    val nagClassificationCode = "RD"
    val nagPaoStartNumber = "56"
    val nagPostcodeLocator = "KL8 7HQ"
    val nagSaoEndSuffix = "JJ"
    val nagLatitude = "4"
    val nagSaoStartNumber = "6473"
    val nagUsrn = "9402538"
    val nagLogicalStatus = "1"
    val nagEasting = "379171.00"
    val nagPaoEndSuffix = "OP"
    val nagStreetDescriptor = "AND ANOTHER STREET DESCRIPTOR"
    val nagUprn = "100010971565"
    val nagNorthing = "412816.00"
    val nagLpiKey = "1610L000014429"
    val nagSaoEndNumber = "6623"
    val nagLongitude = "-2.3162985"
    val nagPaoEndNumber = "7755"
    val nagTownName = "TOWN B"
    val nagLegalName = "ANOTHER LEGAL NAME"
    val nagSaoStartSuffix = "FF"
    val nagPaoText = "A TRAINING CENTRE"
    val nagSaoText = "THE BUILDING NAME"
    val nagPaoStartSuffix = "HH"
    val nagAddressBasePostal = "D"
    val nagLocality = "LOCALITY XYZ"
    val nagLevel = "UP THERE SOME WHERE"


    val pafBuildingNumber = "1"
    val pafUdprn = "19"
    val pafLastUpdateDate = "2016-02-10"
    val pafProOrder = "272650"
    val pafEndDate = "2012-04-25"
    val pafPostcodeType = "S"
    val pafDependentThoroughfare = "throughfare"
    val pafEntryDate = "2012-03-19"
    val pafWelshPostTown = "welsh5"
    val pafDeliveryPointSuffix = "1Q"
    val pafPostcode = "POSTCODE"
    val pafProcessDate = "2016-01-18"
    val pafPoBoxNumber = "6"
    val pafUprn = "1"
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
    val pafStartDate = "2012-04-23"
    val pafRecordIdentifier = "27"


    "cast DataFrame's rows to an LPI key-value Map" in {
      // Given
      val row = Row(
        nagUprn,
        nagPostcodeLocator,
        nagAddressBasePostal,
        nagLatitude,
        nagLongitude,
        nagEasting,
        nagNorthing,
        nagOrganisation,
        nagLegalName,
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
        nagLogicalStatus,
        nagStreetDescriptor,
        nagTownName,
        nagLocality
      )

      val expected = Map(
        "organisation" -> nagOrganisation,
        "officialFlag" -> nagOfficialFlag,
        "classificationCode" -> nagClassificationCode,
        "paoStartNumber" -> nagPaoStartNumber,
        "postcodeLocator" -> nagPostcodeLocator,
        "saoEndSuffix" -> nagSaoEndSuffix,
        "latitude" -> nagLatitude,
        "saoStartNumber" -> nagSaoStartNumber,
        "usrn" -> nagUsrn,
        "logicalStatus" -> nagLogicalStatus,
        "easting" -> nagEasting,
        "paoEndSuffix" -> nagPaoEndSuffix,
        "streetDescriptor" -> nagStreetDescriptor,
        "uprn" -> nagUprn,
        "northing" -> nagNorthing,
        "lpiKey" -> nagLpiKey,
        "saoEndNumber" -> nagSaoEndNumber,
        "longitude" -> nagLongitude,
        "paoEndNumber" -> nagPaoEndNumber,
        "townName" -> nagTownName,
        "legalName" -> nagLegalName,
        "saoStartSuffix" -> nagSaoStartSuffix,
        "paoText" -> nagPaoText,
        "saoText" -> nagSaoText,
        "paoStartSuffix" -> nagPaoStartSuffix,
        "addressBasePostal" -> nagAddressBasePostal,
        "locality" -> nagLocality,
        "level" -> nagLevel
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
        pafEntryDate
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
        "organizationName" -> pafOrganizationName,
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
        "recordIdentifier" -> pafRecordIdentifier
      )

      // When
      val actual = HybridAddressEsDocument.rowToPaf(row)

      // Then
      actual shouldBe expected
    }

  }
}
