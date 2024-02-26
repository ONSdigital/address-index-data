package uk.gov.ons.addressindex.utils

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import uk.gov.ons.addressindex.models.CSVSchemas
import uk.gov.ons.addressindex.readers.AddressIndexFileReader

/**
  * Test that the csv files are joined correctly.
  */
class SqlHelperSpec extends AnyWordSpec with Matchers {

  val format = new java.text.SimpleDateFormat("yyyy-MM-dd")

  "SqlHelper" should {
    "join blpu, organisation, lpi, street, street_descriptor and cross_ref" in {

      // Given
      val blpu = AddressIndexFileReader.readBlpuCSV()
      val classification = AddressIndexFileReader.readClassificationCSV()
      val lpi = AddressIndexFileReader.readLpiCSV()
      val organisation = AddressIndexFileReader.readOrganisationCSV()
      val street = AddressIndexFileReader.readStreetCSV()
      val streetDescriptor = AddressIndexFileReader.readStreetDescriptorCSV()

      // When
      val result = SqlHelper.joinCsvs(blpu, classification, lpi, organisation, street, streetDescriptor).sort("uprn").collect()

      // Then
      result.length shouldBe 9

      val firstLine = result(0)

      firstLine.getLong(0) shouldBe 2L // UPRN
      firstLine.getString(1) shouldBe "KL8 1JQ" // POSTCODE_LOCATOR
      firstLine.getString(2) shouldBe "D" // ADDRESSBASE_POSTAL
      firstLine.get(3) shouldBe Array(-2.3158117F,53.6111710F) // LOCATION
      firstLine.getFloat(4) shouldBe 379203.00F // X_COORDINATE
      firstLine.getFloat(5) shouldBe 412780.00F // Y_COORDINATE
      firstLine.getLong(6) shouldBe 999910971564L // PARENT_UPRN
      firstLine.getShort(7) shouldBe 0 // MULTI_OCC_COUNT
      firstLine.getByte(8) shouldBe 1 // LOGICAL_STATUS
      firstLine.getShort(9) shouldBe 4218 // LOCAL_CUSTODIAN_CODE
      firstLine.getByte(10) shouldBe 1 // RPC
      firstLine.getString(11) shouldBe "SOME COUNCIL" // ORGANISATION
      firstLine.getString(12) shouldBe "THE LEGAL NAME" // LEGAL_NAME
      firstLine.getInt(13) shouldBe 9401385 // USRN
      firstLine.getString(14) shouldBe "1610L000015314" // LPI_KEY
      firstLine.getString(15) shouldBe "ANOTHER BUILDING NAME OR DESCRIPTION" // PAO_TEXT
      firstLine.getShort(16) shouldBe 15 // PAO_START_NUMBER
      firstLine.getString(17) shouldBe "CC" // PAO_START_SUFFIX
      firstLine.getShort(18) shouldBe 9876 // PAO_END_NUMBER
      firstLine.getString(19) shouldBe "AB" // PAO_END_SUFFIX
      firstLine.getString(20) shouldBe "A BUILDING NAME OR DESCRIPTION" // SAO_TEXT
      firstLine.getShort(21) shouldBe 1234 // SAO_START_NUMBER
      firstLine.getString(22) shouldBe "AA" // SAO_START_SUFFIX
      firstLine.getShort(23) shouldBe 5678 // SAO_END_NUMBER
      firstLine.getString(24) shouldBe "BB" // SAO_END_SUFFIX
      firstLine.getString(25) shouldBe "VERTICAL POSITION" // LEVEL
      firstLine.getString(26) shouldBe "Y" // OFFICIAL_FLAG
      firstLine.getByte(27) shouldBe 1 // LOGICAL_STATUS
      firstLine.getByte(28) shouldBe 1 // USRN_MATCH_INDICATOR
      firstLine.getString(29) shouldBe "ENG" // LANGUAGE
      firstLine.getString(30) shouldBe "A STREET DESCRIPTOR" // STREET_DESCRIPTOR
      firstLine.getString(31) shouldBe "TOWNY TOWN" // TOWN_NAME
      firstLine.getString(32) shouldBe "A GREAT LOCALITY" // LOCALITY
      firstLine.getByte(33) shouldBe 8 // STREET_CLASSIFICATION
      firstLine.getDate(34) shouldBe new java.sql.Date(format.parse("2007-10-10").getTime) // LPI START DATE
      firstLine.getDate(35) shouldBe new java.sql.Date(format.parse("2016-03-11").getTime) // LPI LAST UPDATE DATE
      firstLine.getDate(36) shouldBe new java.sql.Date(format.parse("2018-01-11").getTime) // LPI LAST UPDATE DATE
    }

    "join blpu, organisation, lpi, street, street_descriptor and cross_ref without historical data" in {

      // Given
      val blpu = AddressIndexFileReader.readBlpuCSV()
      val classification = AddressIndexFileReader.readClassificationCSV()
      val lpi = AddressIndexFileReader.readLpiCSV()
      val organisation = AddressIndexFileReader.readOrganisationCSV()
      val street = AddressIndexFileReader.readStreetCSV()
      val streetDescriptor = AddressIndexFileReader.readStreetDescriptorCSV()

      // When
      val result = SqlHelper.joinCsvs(blpu, classification, lpi, organisation, street, streetDescriptor, historical = false).sort("uprn").collect()

      // Then
      result.length shouldBe 3

      val firstLine = result(0)

      firstLine.getLong(0) shouldBe 2L // UPRN
      firstLine.getString(1) shouldBe "KL8 1JQ" // POSTCODE_LOCATOR
      firstLine.getString(2) shouldBe "D" // ADDRESSBASE_POSTAL
      firstLine.get(3) shouldBe Array(-2.3158117F,53.6111710F) // LOCATION
      firstLine.getFloat(4) shouldBe 379203.00F // X_COORDINATE
      firstLine.getFloat(5) shouldBe 412780.00F // Y_COORDINATE
      firstLine.getLong(6) shouldBe 999910971564L // PARENT_UPRN
      firstLine.getShort(7) shouldBe 0 // MULTI_OCC_COUNT
      firstLine.getByte(8) shouldBe 1 // LOGICAL_STATUS
      firstLine.getShort(9) shouldBe 4218 // LOCAL_CUSTODIAN_CODE
      firstLine.getByte(10) shouldBe 1 // RPC
      firstLine.getString(11) shouldBe "SOME COUNCIL" // ORGANISATION
      firstLine.getString(12) shouldBe "THE LEGAL NAME" // LEGAL_NAME
      firstLine.getInt(13) shouldBe 9401385 // USRN
      firstLine.getString(14) shouldBe "1610L000015314" // LPI_KEY
      firstLine.getString(15) shouldBe "ANOTHER BUILDING NAME OR DESCRIPTION" // PAO_TEXT
      firstLine.getShort(16) shouldBe 15 // PAO_START_NUMBER
      firstLine.getString(17) shouldBe "CC" // PAO_START_SUFFIX
      firstLine.getShort(18) shouldBe 9876 // PAO_END_NUMBER
      firstLine.getString(19) shouldBe "AB" // PAO_END_SUFFIX
      firstLine.getString(20) shouldBe "A BUILDING NAME OR DESCRIPTION" // SAO_TEXT
      firstLine.getShort(21) shouldBe 1234 // SAO_START_NUMBER
      firstLine.getString(22) shouldBe "AA" // SAO_START_SUFFIX
      firstLine.getShort(23) shouldBe 5678 // SAO_END_NUMBER
      firstLine.getString(24) shouldBe "BB" // SAO_END_SUFFIX
      firstLine.getString(25) shouldBe "VERTICAL POSITION" // LEVEL
      firstLine.getString(26) shouldBe "Y" // OFFICIAL_FLAG
      firstLine.getByte(27) shouldBe 1 // LOGICAL_STATUS
      firstLine.getByte(28) shouldBe 1 // USRN_MATCH_INDICATOR
      firstLine.getString(29) shouldBe "ENG" // LANGUAGE
      firstLine.getString(30) shouldBe "A STREET DESCRIPTOR" // STREET_DESCRIPTOR
      firstLine.getString(31) shouldBe "TOWNY TOWN" // TOWN_NAME
      firstLine.getString(32) shouldBe "A GREAT LOCALITY" // LOCALITY
      firstLine.getByte(33) shouldBe 8 // STREET_CLASSIFICATION
      firstLine.getDate(34) shouldBe new java.sql.Date(format.parse("2007-10-10").getTime) // LPI START DATE
      firstLine.getDate(35) shouldBe new java.sql.Date(format.parse("2016-03-11").getTime) // LPI LAST UPDATE DATE
      firstLine.getDate(36) shouldBe new java.sql.Date(format.parse("2018-01-11").getTime) // LPI LAST UPDATE DATE
    }

    "join blpu, organisation, lpi, street and street_descriptor for English and Welsh address" in {

      // Given
      val blpu = AddressIndexFileReader.readBlpuCSV()
      val classification = AddressIndexFileReader.readClassificationCSV()
      val lpi = AddressIndexFileReader.readLpiCSV()
      val organisation = AddressIndexFileReader.readOrganisationCSV()
      val street = AddressIndexFileReader.readStreetCSV()
      val streetDescriptor = AddressIndexFileReader.readStreetDescriptorCSV()

      // When
      val result = SqlHelper.joinCsvs(blpu, classification, lpi, organisation, street, streetDescriptor).orderBy("locality").orderBy("postcodeLocator").collect()

      // Then
      result.length shouldBe 9

      val firstLine = result(4)

      firstLine.getLong(0) shouldBe 2L // UPRN
      firstLine.getInt(13) shouldBe 9401385 // USRN
      firstLine.getString(32) shouldBe "A GREAT LOCALITY" // LOCALITY

      val secondLine = result(8)

      secondLine.getLong(0) shouldBe 99L // UPRN
      secondLine.getInt(13) shouldBe 9402538 // USRN
      secondLine.getString(32) shouldBe "LOCALITY XYZ" // LOCALITY
    }

    "join blpu, organisation, lpi, street and street_descriptor for English and Welsh address without historical data" in {

      // Given
      val blpu = AddressIndexFileReader.readBlpuCSV()
      val classification = AddressIndexFileReader.readClassificationCSV()
      val lpi = AddressIndexFileReader.readLpiCSV()
      val organisation = AddressIndexFileReader.readOrganisationCSV()
      val street = AddressIndexFileReader.readStreetCSV()
      val streetDescriptor = AddressIndexFileReader.readStreetDescriptorCSV()

      // When
      val result = SqlHelper.joinCsvs(blpu, classification, lpi, organisation, street, streetDescriptor, historical = false).orderBy("uprn", "locality").collect()

      // Then
      result.length shouldBe 3

      val firstLine = result(1)

      firstLine.getLong(0) shouldBe 100010971565L // UPRN
      firstLine.getInt(13) shouldBe 9402538 // USRN
      firstLine.getString(32) shouldBe "FSDF DSFSDF DSF" // LOCALITY

      val secondLine = result(2)

      secondLine.getLong(0) shouldBe 100010971565L // UPRN
      secondLine.getInt(13) shouldBe 9402538 // USRN
      secondLine.getString(32) shouldBe "LOCALITY XYZ" // LOCALITY
    }

    "aggregate relatives from hierarchy table" in {
      // Given
      val hierarchy = AddressIndexFileReader.readHierarchyCSV()

      // When
      val result = SqlHelper.aggregateHierarchyInformation(hierarchy).orderBy("primaryUprn", "level").collect()

      // Then
      result.length shouldBe 5
      val firstLine = result(0)
      firstLine.getLong(0) shouldBe 1L
      firstLine.getInt(1) shouldBe 1
      firstLine.getAs[Array[Long]](2) shouldBe Array(1L)
      firstLine.getAs[Array[Long]](3) shouldBe Array()

      val secondLine = result(1)
      secondLine.getLong(0) shouldBe 1L
      secondLine.getInt(1) shouldBe 2
      secondLine.getAs[Array[Long]](2) shouldBe Array(2L, 3L, 4L)
      secondLine.getAs[Array[Long]](3) shouldBe Array(1L, 1L, 1L)

      val thirdLine = result(2)
      thirdLine.getLong(0) shouldBe 1L
      thirdLine.getInt(1) shouldBe 3
      thirdLine.getAs[Array[Long]](2) shouldBe Array(5L, 6L, 7L, 8L, 9L)
      thirdLine.getAs[Array[Long]](3) shouldBe Array(2L, 2L, 2L, 3L, 3L)

      val forthLine = result(3)
      forthLine.getLong(0) shouldBe 10L
      forthLine.getInt(1) shouldBe 1
      forthLine.getAs[Array[Long]](2) shouldBe Array(10L)
      forthLine.getAs[Array[Long]](3) shouldBe Array()

      val fifthLine = result(4)
      fifthLine.getLong(0) shouldBe 10L
      fifthLine.getInt(1) shouldBe 2
      fifthLine.getAs[Array[Long]](2) shouldBe Array(11L, 12L)
      fifthLine.getAs[Array[Long]](3) shouldBe Array(10L, 10L)
    }

    "aggregate crossrefs from crossref table" in {
      // Given
      val crossref = AddressIndexFileReader.readCrossrefCSV()

      // When
      val result = SqlHelper.aggregateCrossRefInformation(crossref).orderBy("uprn", "crossReference").collect()

      // Then
      result.length shouldBe 10
      val firstLine = result(0)
      firstLine.getAs[Long]("uprn") shouldBe 2L
      firstLine.getAs[String]("crossReference") shouldBe "E04000324"
      firstLine.getAs[String]("source") shouldBe "7666MI"

      val thirdLine = result(2)
      thirdLine.getAs[Long]("uprn") shouldBe 10090373276L
      thirdLine.getAs[String]("crossReference") shouldBe "E05001602"
      thirdLine.getAs[String]("source") shouldBe "7666OW"

      val fifthLine = result(4)
      fifthLine.getAs[Long]("uprn") shouldBe 100010971565L
      fifthLine.getAs[String]("crossReference") shouldBe "44654912"
      fifthLine.getAs[String]("source") shouldBe "7666VC"

      val seventhLine = result(6)
      seventhLine.getAs[Long]("uprn") shouldBe 100010971565L
      seventhLine.getAs[String]("crossReference") shouldBe "E01001700"
      seventhLine.getAs[String]("source") shouldBe "7666OL"
    }

    "aggregate information from paf and nag to construct a single table containing grouped documents" in {

      // Given
      val paf = SparkProvider.sparkContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .schema(CSVSchemas.postcodeAddressFileSchema)
        .load("batch/src/test/resources/csv/delivery_point/hybrid_test.csv")

      val blpu = AddressIndexFileReader.readBlpuCSV()
      val classification = AddressIndexFileReader.readClassificationCSV()
      val lpi = AddressIndexFileReader.readLpiCSV()
      val organisation = AddressIndexFileReader.readOrganisationCSV()
      val street = AddressIndexFileReader.readStreetCSV()
      val streetDescriptor = AddressIndexFileReader.readStreetDescriptorCSV()

      val nag = SqlHelper.joinCsvs(blpu, classification, lpi, organisation, street, streetDescriptor)

      val expectedFirstRelations = Array(
        Map(
          "level" -> 1,
          "siblings" -> Array(1).deep,
          "parents" -> Array().deep
        ),
        Map(
          "level" -> 2,
          "siblings" -> Array(2, 3, 4).deep,
          "parents" -> Array(1, 1, 1).deep
        ),
        Map(
          "level" -> 3,
          "siblings" -> Array(5, 6, 7, 8, 9).deep,
          "parents" -> Array(2, 2, 2, 3, 3).deep
        )
      )

      val expectedSecondCrossRefs = Array(
        Map(
          "crossReference" -> "44654912",
          "source" -> "7666VC"
        ),
        Map(
          "crossReference" -> "44654913",
          "source" -> "7666VN"
        ),
        Map(
          "crossReference" -> "E05001602",
          "source" -> "7666MI"
        ),
        Map(
          "crossReference" -> "E03001901",
          "source" -> "7666OP"
        ),
        Map(
          "crossReference" -> "E01001700",
          "source" -> "7666OL"
        ),
        Map(
          "crossReference" -> "E03801409",
          "source" -> "7666OU"
        )
      )

      // When
      val result = SqlHelper.aggregateHybridIndex(paf, nag).sortBy(_.uprn).collect()

      // Then
      result.length shouldBe 4

      val firstResult = result(0)
      firstResult.uprn shouldBe 2L
      firstResult.classificationCode shouldBe Some("RD")
      firstResult.postcodeOut shouldBe "KL8"
      firstResult.postcodeIn shouldBe "1JQ"
      firstResult.parentUprn shouldBe 1L
      firstResult.relatives.length shouldBe 3
      firstResult.crossRefs.length shouldBe 2
      firstResult.lpi.size shouldBe 1
      firstResult.paf shouldBe empty

      val secondResult = result(3)
      secondResult.uprn shouldBe 100010971565L
      secondResult.classificationCode shouldBe Some("RD")
      secondResult.postcodeOut shouldBe "PO15"
      secondResult.postcodeIn shouldBe "5RZ"
      secondResult.parentUprn shouldBe 0L
      secondResult.relatives.length shouldBe 0
      secondResult.crossRefs.length shouldBe 6
      secondResult.lpi.size shouldBe 3
      secondResult.paf.size shouldBe 1

      // Hierarchy test
      firstResult.relatives.toList.sortBy(_.getOrElse("level", 0).toString) shouldBe expectedFirstRelations.toList

      // CrossRefs Test
      secondResult.crossRefs.toList.sortBy(_("crossReference")) shouldBe expectedSecondCrossRefs.toList.sortBy(_("crossReference"))

      List(secondResult.lpi.head("lpiKey")) should contain oneOf("1610L000056911","1610L000056913","1610L000014429")
      List(secondResult.lpi(1)("lpiKey")) should contain oneOf("1610L000056911","1610L000056913","1610L000014429")
      List(secondResult.lpi(2)("lpiKey")) should contain oneOf("1610L000056911","1610L000056913","1610L000014429")

      secondResult.paf.head("recordIdentifier") shouldBe 27
    }

    "aggregate information from paf and nag to construct a single table containing grouped documents without historical data" in {

      // This test assures us that a uprn that has only historical lpi's but has a paf will not make it to the results
      // Given
      val paf1 = SparkProvider.sparkContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .schema(CSVSchemas.postcodeAddressFileSchema)
        .load("batch/src/test/resources/csv/delivery_point/hybrid_test.csv")

      val paf2 = SparkProvider.sparkContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .schema(CSVSchemas.postcodeAddressFileSchema)
        .load("batch/src/test/resources/csv/delivery_point/hybrid_test_hist.csv")

      val blpu = AddressIndexFileReader.readBlpuCSV()
      val classification = AddressIndexFileReader.readClassificationCSV()
      val lpi = AddressIndexFileReader.readLpiCSV()
      val organisation = AddressIndexFileReader.readOrganisationCSV()
      val street = AddressIndexFileReader.readStreetCSV()
      val streetDescriptor = AddressIndexFileReader.readStreetDescriptorCSV()

      val nag = SqlHelper.joinCsvs(blpu, classification, lpi, organisation, street, streetDescriptor, historical = false)

      val expectedFirstRelations = Array(
        Map(
          "level" -> 1,
          "siblings" -> Array(1).deep,
          "parents" -> Array().deep
        ),
        Map(
          "level" -> 2,
          "siblings" -> Array(2, 3, 4).deep,
          "parents" -> Array(1, 1, 1).deep
        ),
        Map(
          "level" -> 3,
          "siblings" -> Array(5, 6, 7, 8, 9).deep,
          "parents" -> Array(2, 2, 2, 3, 3).deep
        )
      )

      val expectedSecondCrossRefs = Array(
        Map(
          "crossReference" -> "44654912",
          "source" -> "7666VC"
        ),
        Map(
          "crossReference" -> "44654913",
          "source" -> "7666VN"
        ),
        Map(
          "crossReference" -> "E05001602",
          "source" -> "7666MI"
        ),
        Map(
          "crossReference" -> "E03001901",
          "source" -> "7666OP"
        ),
        Map(
          "crossReference" -> "E01001700",
          "source" -> "7666OL"
        ),
        Map(
          "crossReference" -> "E03801409",
          "source" -> "7666OU"
        )
      )

      // When
      val result = SqlHelper.aggregateHybridIndex(paf1.union(paf2), nag, historical = false).sortBy(_.uprn).collect()

      // Then
      result.length shouldBe 2

      val firstResult = result(0)
      firstResult.uprn shouldBe 2L
      firstResult.classificationCode shouldBe Some("RD")
      firstResult.postcodeOut shouldBe "KL8"
      firstResult.postcodeIn shouldBe "1JQ"
      firstResult.parentUprn shouldBe 1L
      firstResult.relatives.length shouldBe 3
      firstResult.crossRefs.length shouldBe 2
      firstResult.lpi.size shouldBe 1
      firstResult.paf shouldBe empty

      val secondResult = result(1)
      secondResult.uprn shouldBe 100010971565L
      secondResult.classificationCode shouldBe Some("RD")
      secondResult.postcodeOut shouldBe "PO15"
      secondResult.postcodeIn shouldBe "5RZ"
      secondResult.parentUprn shouldBe 0L
      secondResult.relatives.length shouldBe 0
      secondResult.crossRefs.length shouldBe 6
      secondResult.lpi.size shouldBe 2
      secondResult.paf.size shouldBe 1

      // Hierarchy test
      firstResult.relatives.toList.sortBy(_.getOrElse("level", 0).toString) shouldBe expectedFirstRelations.toList

      // CrossRefs Test
      secondResult.crossRefs.toList.sortBy(_("crossReference")) shouldBe expectedSecondCrossRefs.toList.sortBy(_("crossReference"))

      List(secondResult.lpi.head("lpiKey")) should contain oneOf("1610L000056913","1610L000014429")
      List(secondResult.lpi(1)("lpiKey")) should contain oneOf("1610L000056913","1610L000014429")

      secondResult.paf.head("recordIdentifier") shouldBe 27
    }


    "aggregate information from skinny paf and nag to construct a single table containing grouped documents" in {

      // Given
      val paf = SparkProvider.sparkContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .schema(CSVSchemas.postcodeAddressFileSchema)
        .load("batch/src/test/resources/csv/delivery_point/hybrid_test.csv")

      val blpu = AddressIndexFileReader.readBlpuCSV()
      val classification = AddressIndexFileReader.readClassificationCSV()
      val lpi = AddressIndexFileReader.readLpiCSV()
      val organisation = AddressIndexFileReader.readOrganisationCSV()
      val street = AddressIndexFileReader.readStreetCSV()
      val streetDescriptor = AddressIndexFileReader.readStreetDescriptorCSV()

      val nag = SqlHelper.joinCsvs(blpu, classification, lpi, organisation, street, streetDescriptor)

      // When
      val result = SqlHelper.aggregateHybridSkinnyIndex(paf, nag).sortBy(_.uprn).collect()

      // Then
      result.length shouldBe 4

      val firstResult = result(0)
      firstResult.uprn shouldBe 2L
      firstResult.classificationCode shouldBe Some("RD")
      firstResult.parentUprn shouldBe 1L
      firstResult.lpi.size shouldBe 1
      firstResult.paf shouldBe empty
      firstResult.addressEntryId shouldBe Some(100000034563800L)

      val secondResult = result(3)
      secondResult.uprn shouldBe 100010971565L
      secondResult.classificationCode shouldBe Some("RD")
      secondResult.parentUprn shouldBe 0L
      secondResult.lpi.size shouldBe 3
      secondResult.paf.size shouldBe 1
      secondResult.addressEntryId shouldBe Some(100000034563798L)

    }

    "aggregate information from skinny paf and nag to construct a single table containing grouped documents without historical data" in {

      // This test assures us that a uprn that has only historical lpi's but has a paf will not make it to the results
      // Given
      val paf1 = SparkProvider.sparkContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .schema(CSVSchemas.postcodeAddressFileSchema)
        .load("batch/src/test/resources/csv/delivery_point/hybrid_test.csv")

      val paf2 = SparkProvider.sparkContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .schema(CSVSchemas.postcodeAddressFileSchema)
        .load("batch/src/test/resources/csv/delivery_point/hybrid_test_hist.csv")

      val blpu = AddressIndexFileReader.readBlpuCSV()
      val classification = AddressIndexFileReader.readClassificationCSV()
      val lpi = AddressIndexFileReader.readLpiCSV()
      val organisation = AddressIndexFileReader.readOrganisationCSV()
      val street = AddressIndexFileReader.readStreetCSV()
      val streetDescriptor = AddressIndexFileReader.readStreetDescriptorCSV()

      val nag = SqlHelper.joinCsvs(blpu, classification, lpi, organisation, street, streetDescriptor, historical = false)

      // When
      val result = SqlHelper.aggregateHybridSkinnyIndex(paf1.union(paf2), nag,historical = false).sortBy(_.uprn).collect()

      // Then
      result.length shouldBe 2

      val firstResult = result(0)
      firstResult.uprn shouldBe 2L
      firstResult.classificationCode shouldBe Some("RD")
      firstResult.parentUprn shouldBe 1L
      firstResult.lpi.size shouldBe 1
      firstResult.paf shouldBe empty
      firstResult.addressEntryId shouldBe Some(100000034563800L)

      val secondResult = result(1)
      secondResult.uprn shouldBe 100010971565L
      secondResult.classificationCode shouldBe Some("RD")
      secondResult.parentUprn shouldBe 0L
      secondResult.lpi.size shouldBe 2
      secondResult.paf.size shouldBe 1
      secondResult.addressEntryId shouldBe Some(100000034563798L)
    }


  }
}