package uk.gov.ons.addressindex.utils

import org.apache.spark.sql.types._
import org.scalatest.{Matchers, WordSpec}
import uk.gov.ons.addressindex.models.{CSVSchemas, HybridAddressEsDocument}
import uk.gov.ons.addressindex.readers.AddressIndexFileReader
import org.apache.spark.sql._

/**
  * Test that the csv files are joined correctly.
  */
class SqlHelperSpec extends WordSpec with Matchers {

  "SqlHelper" should {
    "join blpu, organisation, lpi, street, street_descriptor and cross_ref" in {

      // Given
      val blpu = AddressIndexFileReader.readBlpuCSV()
      val lpi = AddressIndexFileReader.readLpiCSV()
      val organisation = AddressIndexFileReader.readOrganisationCSV()
      val classification = AddressIndexFileReader.readClassificationCSV()
      val street = AddressIndexFileReader.readStreetCSV()
      val streetDescriptor = AddressIndexFileReader.readStreetDescriptorCSV()
      val crossRef = AddressIndexFileReader.readCrossrefCSV()

      // When
      val result = SqlHelper.joinCsvs(blpu, lpi, organisation, classification, street, streetDescriptor, crossRef).collect()

      // Then
      result.length shouldBe 4

      val firstLine = result(1)

      firstLine.getLong(0) shouldBe 100010971564L // UPRN
      firstLine.getString(1) shouldBe "KL8 1JQ" // POSTCODE_LOCATOR
      firstLine.getString(2) shouldBe "D" // ADDRESSBASE_POSTAL
      firstLine.get(3) shouldBe (Array(-2.3158117F,53.6111710F)) // LOCATION
      firstLine.getFloat(4) shouldBe 379203.00F // X_COORDINATE
      firstLine.getFloat(5) shouldBe 412780.00F // Y_COORDINATE
      firstLine.getLong(6) shouldBe 999910971564L // PARENT_UPRN
      firstLine.getShort(7) shouldBe 0 // MULTI_OCC_COUNT
      firstLine.getByte(8) shouldBe 1 // LOGICAL_STATUS
      firstLine.getShort(9) shouldBe 4218 // LOCAL_CUSTODIAN_CODE
      firstLine.getByte(10) shouldBe 1 // RPC
      firstLine.getString(11) shouldBe "SOME COUNCIL" // ORGANISATION
      firstLine.getString(12) shouldBe "THE LEGAL NAME" // LEGAL_NAME
      firstLine.getString(13) shouldBe "AddressBase Premium Classification Scheme" // CLASS_SCHEME
      firstLine.getString(14) shouldBe "RD" // CLASSIFICATION_CODE
      firstLine.getInt(15) shouldBe 9401385 // USRN
      firstLine.getString(16) shouldBe "1610L000015314" // LPI_KEY
      firstLine.getString(17) shouldBe "ANOTHER BUILDING NAME OR DESCRIPTION" // PAO_TEXT
      firstLine.getShort(18) shouldBe 15 // PAO_START_NUMBER
      firstLine.getString(19) shouldBe "CC" // PAO_START_SUFFIX
      firstLine.getShort(20) shouldBe 9876 // PAO_END_NUMBER
      firstLine.getString(21) shouldBe "AB" // PAO_END_SUFFIX
      firstLine.getString(22) shouldBe "A BUILDING NAME OR DESCRIPTION" // SAO_TEXT
      firstLine.getShort(23) shouldBe 1234 // SAO_START_NUMBER
      firstLine.getString(24) shouldBe "AA" // SAO_START_SUFFIX
      firstLine.getShort(25) shouldBe 5678 // SAO_END_NUMBER
      firstLine.getString(26) shouldBe "BB" // SAO_END_SUFFIX
      firstLine.getString(27) shouldBe "VERTICAL POSITION" // LEVEL
      firstLine.getString(28) shouldBe "Y" // OFFICIAL_FLAG
      firstLine.getByte(29) shouldBe 1 // LOGICAL_STATUS
      firstLine.getByte(30) shouldBe 1 // USRN_MATCH_INDICATOR
      firstLine.getString(31) shouldBe "ENG" // LANGUAGE
      firstLine.getString(32) shouldBe "A STREET DESCRIPTOR" // STREET_DESCRIPTOR
      firstLine.getString(33) shouldBe "TOWNY TOWN" // TOWN_NAME
      firstLine.getString(34) shouldBe "A GREAT LOCALITY" // LOCALITY
      firstLine.getByte(35) shouldBe 8 // STREET_CLASSIFICATION
      firstLine.getString(36) shouldBe "E04000324" // CROSS_REFERENCE
      firstLine.getString(37) shouldBe "7666MI" // SOURCE
      firstLine.getString(38) shouldBe "SOME COUNCIL 1234AA-5678BB A BUILDING NAME OR DESCRIPTION ANOTHER BUILDING NAME OR DESCRIPTION 15CC-9876AB A STREET DESCRIPTOR A GREAT LOCALITY TOWNY TOWN KL8 1JQ"
    }

    "join blpu, organisation, lpi, street and street_descriptor for English and Welsh address" in {

      // Given
      val blpu = AddressIndexFileReader.readBlpuCSV()
      val lpi = AddressIndexFileReader.readLpiCSV()
      val organisation = AddressIndexFileReader.readOrganisationCSV()
      val classification = AddressIndexFileReader.readClassificationCSV()
      val street = AddressIndexFileReader.readStreetCSV()
      val streetDescriptor = AddressIndexFileReader.readStreetDescriptorCSV()
      val crossRef = AddressIndexFileReader.readCrossrefCSV()

      // When
      val result = SqlHelper.joinCsvs(blpu, lpi, organisation, classification, street, streetDescriptor, crossRef).orderBy("uprn").collect()

      // Then
      result.length shouldBe 4

      val firstLine = result(2)

      firstLine.getLong(0) shouldBe 100010971565L // UPRN
      firstLine.getInt(15) shouldBe 9402538 // USRN
      firstLine.getString(34) shouldBe "FSDF DSFSDF DSF" // LOCALITY

      val secondLine = result(1)

      secondLine.getLong(0) shouldBe 100010971565L // UPRN
      secondLine.getInt(15) shouldBe 9402538 // USRN
      secondLine.getString(34) shouldBe "LOCALITY XYZ" // LOCALITY
    }

    "aggregate information from paf and nag to construct a single table containing grouped documents" in {

      /**
        * NAG CSV file schema
        */
      val nagFileSchema = StructType(Seq(
        StructField("uprn", LongType, nullable = false),
        StructField("postcodeLocator", StringType, nullable = false),
        StructField("addressBasePostal", StringType, nullable = false),
        StructField("latitude", FloatType, nullable = false),
        StructField("longitude", FloatType, nullable = false),
        StructField("easting", FloatType, nullable = false),
        StructField("northing", FloatType, nullable = false),
        StructField("parentUprn", LongType, nullable = false),
        StructField("multiOccCount", ShortType, nullable = false),
        StructField("blpuLogicalStatus", ByteType, nullable = false),
        StructField("localCustodianCode", ShortType, nullable = false),
        StructField("rpc", ByteType, nullable = false),
        StructField("organisation", StringType, nullable = false),
        StructField("legalName", StringType, nullable = false),
        StructField("classScheme", StringType, nullable = false),
        StructField("classificationCode", StringType, nullable = false),
        StructField("usrn", IntegerType, nullable = false),
        StructField("lpiKey", StringType, nullable = false),
        StructField("paoText", StringType, nullable = false),
        StructField("paoStartNumber", ShortType, nullable = false),
        StructField("paoStartSuffix", StringType, nullable = false),
        StructField("paoEndNumber", ShortType, nullable = false),
        StructField("paoEndSuffix", StringType, nullable = false),
        StructField("saoText", StringType, nullable = false),
        StructField("saoStartNumber", ShortType, nullable = false),
        StructField("saoStartSuffix", StringType, nullable = false),
        StructField("saoEndNumber", ShortType, nullable = false),
        StructField("saoEndSuffix", StringType, nullable = false),
        StructField("level", StringType, nullable = false),
        StructField("officialFlag", StringType, nullable = false),
        StructField("lpiLogicalStatus", IntegerType, nullable = false),
        StructField("usrnMatchIndicator", ByteType, nullable = false),
        StructField("language", StringType, nullable = false),
        StructField("streetDescriptor", StringType, nullable = false),
        StructField("townName", StringType, nullable = false),
        StructField("locality", StringType, nullable = false),
        StructField("streetClassification", ByteType, nullable = false),
        StructField("crossReference", StringType, nullable = false),
        StructField("source", StringType, nullable = false)
      ))

      // Given
      val paf = SparkProvider.sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .schema(CSVSchemas.postcodeAddressFileSchema)
        .load("batch/src/test/resources/csv/delivery_point/hybrid_test.csv")

//      paf.registerTempTable("paf")
//
//      val concatPaf = SparkProvider.sqlContext.sql(
//        s"""SELECT
//              *,
//              concatPaf(trim(poBoxNumber),
//              cast(buildingNumber as String),
//              trim(dependentThoroughfare),
//              trim(welshDependentThoroughfare),
//              trim(thoroughfare),
//              trim(welshThoroughfare),
//              trim(departmentName),
//              trim(organisationName),
//              trim(subBuildingName),
//              trim(buildingName),
//              trim(doubleDependentLocality),
//              trim(welshDoubleDependentLocality),
//              trim(dependentLocality),
//              trim(welshDependentLocality),
//              trim(postTown),
//              trim(welshPostTown),
//              trim(postcode)) as pafAll
//            FROM paf""").na.fill("")

      val nag = SparkProvider.sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .schema(nagFileSchema)
        .load("batch/src/test/resources/csv/nag/hybrid_test.csv")
//        .registerTempTable("nag")

      val nagUpdate = nag.withColumn("location", array(nag.col("longitude"),nag.col("latitude")))

//      val nagUpdate = SparkProvider.sqlContext.sql(
//        s"""SELECT
//          uprn,
//          postcodeLocator,
//          addressBasePostal,
//          array(longitude, latitude) as location,
//          easting,
//          northing,
//          parentUprn,
//          multiOccCount,
//          blpuLogicalStatus,
//          localCustodianCode,
//          rpc,
//          organisation,
//          legalName,
//          classScheme,
//          classificationCode,
//          usrn,
//          lpiKey,
//          paoText,
//          paoStartNumber,
//          paoStartSuffix,
//          paoEndNumber,
//          paoEndSuffix,
//          saoText,
//          saoStartNumber,
//          saoStartSuffix,
//          saoEndNumber,
//          saoEndSuffix,
//          level,
//          officialFlag,
//          lpiLogicalStatus,
//          usrnMatchIndicator,
//          language,
//          streetDescriptor,
//          townName,
//          locality,
//          streetClassification,
//          crossReference,
//          source,
//          concatNag(nvl(cast(saoStartNumber as String), ""),
//                    nvl(cast(saoEndNumber as String), ""),
//                    saoEndSuffix,
//                    saoStartSuffix,
//                    saoText,
//                    organisation,
//                    nvl(cast(paoStartNumber as String), ""),
//                    paoStartSuffix,
//                    nvl(cast(paoEndNumber as String), ""),
//                    paoEndSuffix,
//                    paoText,
//                    streetDescriptor,
//                    townName,
//                    locality,
//                    postcodeLocator) as nagAll
//        FROM nag""").na.fill("")

      // When
      val result = SqlHelper.aggregateHybridIndex(paf, nag).sortBy(_.uprn).collect()

      // Then
      result.length shouldBe 3

      val firstResult = result(0)
      firstResult.uprn shouldBe 1L
      firstResult.lpi.size shouldBe 2
      firstResult.paf.size shouldBe 1

      firstResult.lpi(0)("lpiKey") shouldBe "1610L000056911"
      firstResult.lpi(1)("lpiKey") shouldBe "1610L000015314"

      firstResult.paf(0)("recordIdentifier") shouldBe 27

      val secondResult = result(1)
      secondResult.uprn shouldBe 100010971565L
      secondResult.lpi.size shouldBe 1
      secondResult.paf shouldBe empty
    }
  }
}