package uk.gov.ons.addressindex.utils

import org.scalatest.{Matchers, WordSpec}
import uk.gov.ons.addressindex.models.CSVSchemas
import uk.gov.ons.addressindex.readers.AddressIndexFileReader

/**
  * Test that the csv files are joined correctly.
  */
class SqlHelperSpec extends WordSpec with Matchers {

  val format = new java.text.SimpleDateFormat("yyyy-MM-dd")

  "SqlHelper" should {
    "join blpu, organisation, lpi, street, street_descriptor and cross_ref" in {

      // Given
      val blpu = AddressIndexFileReader.readBlpuCSV()
      val lpi = AddressIndexFileReader.readLpiCSV()
      val organisation = AddressIndexFileReader.readOrganisationCSV()
      val classification = AddressIndexFileReader.readClassificationCSV()
      val street = AddressIndexFileReader.readStreetCSV()
      val streetDescriptor = AddressIndexFileReader.readStreetDescriptorCSV()

      // When
      val result = SqlHelper.joinCsvs(blpu, lpi, organisation, classification, street, streetDescriptor).sort("uprn").collect()

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
      firstLine.getDate(36) shouldBe new java.sql.Date(format.parse("2007-10-10").getTime) // LPI START DATE
      firstLine.getDate(37) shouldBe new java.sql.Date(format.parse("2016-03-11").getTime) // LPI LAST UPDATE DATE
      firstLine.getDate(38) shouldBe new java.sql.Date(format.parse("2018-01-11").getTime) // LPI LAST UPDATE DATE
    }

    "join blpu, organisation, lpi, street, street_descriptor and cross_ref without historical data" in {

      // Given
      val blpu = AddressIndexFileReader.readBlpuCSV()
      val lpi = AddressIndexFileReader.readLpiCSV()
      val organisation = AddressIndexFileReader.readOrganisationCSV()
      val classification = AddressIndexFileReader.readClassificationCSV()
      val street = AddressIndexFileReader.readStreetCSV()
      val streetDescriptor = AddressIndexFileReader.readStreetDescriptorCSV()

      // When
      val result = SqlHelper.joinCsvs(blpu, lpi, organisation, classification, street, streetDescriptor, historical = false).sort("uprn").collect()

      // Then
      result.length shouldBe 6

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
      firstLine.getDate(36) shouldBe new java.sql.Date(format.parse("2007-10-10").getTime) // LPI START DATE
      firstLine.getDate(37) shouldBe new java.sql.Date(format.parse("2016-03-11").getTime) // LPI LAST UPDATE DATE
      firstLine.getDate(38) shouldBe new java.sql.Date(format.parse("2018-01-11").getTime) // LPI LAST UPDATE DATE
    }

    "join blpu, organisation, lpi, street and street_descriptor for English and Welsh address" in {

      // Given
      val blpu = AddressIndexFileReader.readBlpuCSV()
      val lpi = AddressIndexFileReader.readLpiCSV()
      val organisation = AddressIndexFileReader.readOrganisationCSV()
      val classification = AddressIndexFileReader.readClassificationCSV()
      val street = AddressIndexFileReader.readStreetCSV()
      val streetDescriptor = AddressIndexFileReader.readStreetDescriptorCSV()

      // When
      val result = SqlHelper.joinCsvs(blpu, lpi, organisation, classification, street, streetDescriptor).orderBy("locality").orderBy("postcodeLocator").collect()

      // Then
      result.length shouldBe 9

      val firstLine = result(4)

      firstLine.getLong(0) shouldBe 2L // UPRN
      firstLine.getInt(15) shouldBe 9401385 // USRN
      firstLine.getString(34) shouldBe "A GREAT LOCALITY" // LOCALITY

      val secondLine = result(8)

      secondLine.getLong(0) shouldBe 99L // UPRN
      secondLine.getInt(15) shouldBe 9402538 // USRN
      secondLine.getString(34) shouldBe "LOCALITY XYZ" // LOCALITY
    }

    "join blpu, organisation, lpi, street and street_descriptor for English and Welsh address without historical data" in {

      // Given
      val blpu = AddressIndexFileReader.readBlpuCSV()
      val lpi = AddressIndexFileReader.readLpiCSV()
      val organisation = AddressIndexFileReader.readOrganisationCSV()
      val classification = AddressIndexFileReader.readClassificationCSV()
      val street = AddressIndexFileReader.readStreetCSV()
      val streetDescriptor = AddressIndexFileReader.readStreetDescriptorCSV()

      // When
      val result = SqlHelper.joinCsvs(blpu, lpi, organisation, classification, street, streetDescriptor, historical = false).orderBy("uprn", "locality").collect()

      // Then
      result.length shouldBe 6

      val firstLine = result(4)

      firstLine.getLong(0) shouldBe 100010971565L // UPRN
      firstLine.getInt(15) shouldBe 9402538 // USRN
      firstLine.getString(34) shouldBe "FSDF DSFSDF DSF" // LOCALITY

      val secondLine = result(5)

      secondLine.getLong(0) shouldBe 100010971565L // UPRN
      secondLine.getInt(15) shouldBe 9402538 // USRN
      secondLine.getString(34) shouldBe "LOCALITY XYZ" // LOCALITY
    }

    "aggregate relatives from hierarchy table" in {
      // Given
      val hierarchy = AddressIndexFileReader.readHierarchyCSV()

      // When
      val result = SqlHelper.aggregateHierarchyInformation(hierarchy).orderBy("primaryUprn", "level").collect()

      // Then
      result.length shouldBe 5
      val firstLine = result(0)
      firstLine.getLong(0) shouldBe 1l
      firstLine.getInt(1) shouldBe 1
      firstLine.getAs[Array[Long]](2) shouldBe Array(1l)
      firstLine.getAs[Array[Long]](3) shouldBe Array()

      val secondLine = result(1)
      secondLine.getLong(0) shouldBe 1l
      secondLine.getInt(1) shouldBe 2
      secondLine.getAs[Array[Long]](2) shouldBe Array(2l, 3l, 4l)
      secondLine.getAs[Array[Long]](3) shouldBe Array(1l, 1l, 1l)

      val thirdLine = result(2)
      thirdLine.getLong(0) shouldBe 1l
      thirdLine.getInt(1) shouldBe 3
      thirdLine.getAs[Array[Long]](2) shouldBe Array(5l, 6l, 7l, 8l, 9l)
      thirdLine.getAs[Array[Long]](3) shouldBe Array(2l, 2l, 2l, 3l, 3l)

      val forthLine = result(3)
      forthLine.getLong(0) shouldBe 10l
      forthLine.getInt(1) shouldBe 1
      forthLine.getAs[Array[Long]](2) shouldBe Array(10l)
      forthLine.getAs[Array[Long]](3) shouldBe Array()

      val fifthLine = result(4)
      fifthLine.getLong(0) shouldBe 10l
      fifthLine.getInt(1) shouldBe 2
      fifthLine.getAs[Array[Long]](2) shouldBe Array(11l, 12l)
      fifthLine.getAs[Array[Long]](3) shouldBe Array(10l, 10l)
    }

    "aggregate crossrefs from crossref table" in {
      // Given
      val crossref = AddressIndexFileReader.readCrossrefCSV()

      // When
      val result = SqlHelper.aggregateCrossRefInformation(crossref).orderBy("uprn", "crossReference").collect()

      // Then
      result.length shouldBe 8
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
      fifthLine.getAs[String]("crossReference") shouldBe "E01001700"
      fifthLine.getAs[String]("source") shouldBe "7666OL"

      val seventhLine = result(6)
      seventhLine.getAs[Long]("uprn") shouldBe 100010971565L
      seventhLine.getAs[String]("crossReference") shouldBe "E03801409"
      seventhLine.getAs[String]("source") shouldBe "7666OU"
    }

    "aggregate information from paf and nag to construct a single table containing grouped documents" in {

      // Given
      val paf = SparkProvider.sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .schema(CSVSchemas.postcodeAddressFileSchema)
        .load("batch/src/test/resources/csv/delivery_point/hybrid_test.csv")

      val blpu = AddressIndexFileReader.readBlpuCSV()
      val lpi = AddressIndexFileReader.readLpiCSV()
      val organisation = AddressIndexFileReader.readOrganisationCSV()
      val classification = AddressIndexFileReader.readClassificationCSV()
      val street = AddressIndexFileReader.readStreetCSV()
      val streetDescriptor = AddressIndexFileReader.readStreetDescriptorCSV()

      val nag = SqlHelper.joinCsvs(blpu, lpi, organisation, classification, street, streetDescriptor)

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
      firstResult.postcodeOut shouldBe "KL8"
      firstResult.postcodeIn shouldBe "1JQ"
      firstResult.parentUprn shouldBe 1l
      firstResult.relatives.length shouldBe 3
      firstResult.crossRefs.length shouldBe 2
      firstResult.lpi.size shouldBe 1
      firstResult.paf shouldBe empty

      val secondResult = result(3)
      secondResult.uprn shouldBe 100010971565L
      secondResult.postcodeOut shouldBe "PO15"
      secondResult.postcodeIn shouldBe "5RZ"
      secondResult.parentUprn shouldBe 0L
      secondResult.relatives.length shouldBe 0
      secondResult.crossRefs.length shouldBe 4
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
      val paf1 = SparkProvider.sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .schema(CSVSchemas.postcodeAddressFileSchema)
        .load("batch/src/test/resources/csv/delivery_point/hybrid_test.csv")

      val paf2 = SparkProvider.sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .schema(CSVSchemas.postcodeAddressFileSchema)
        .load("batch/src/test/resources/csv/delivery_point/hybrid_test_hist.csv")

      val blpu = AddressIndexFileReader.readBlpuCSV()
      val lpi = AddressIndexFileReader.readLpiCSV()
      val organisation = AddressIndexFileReader.readOrganisationCSV()
      val classification = AddressIndexFileReader.readClassificationCSV()
      val street = AddressIndexFileReader.readStreetCSV()
      val streetDescriptor = AddressIndexFileReader.readStreetDescriptorCSV()

      val nag = SqlHelper.joinCsvs(blpu, lpi, organisation, classification, street, streetDescriptor, historical = false)

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
      result.length shouldBe 3

      val firstResult = result(0)
      firstResult.uprn shouldBe 2L
      firstResult.postcodeOut shouldBe "KL8"
      firstResult.postcodeIn shouldBe "1JQ"
      firstResult.parentUprn shouldBe 1l
      firstResult.relatives.length shouldBe 3
      firstResult.crossRefs.length shouldBe 2
      firstResult.lpi.size shouldBe 1
      firstResult.paf shouldBe empty

      val secondResult = result(2)
      secondResult.uprn shouldBe 100010971565L
      secondResult.postcodeOut shouldBe "PO15"
      secondResult.postcodeIn shouldBe "5RZ"
      secondResult.parentUprn shouldBe 0L
      secondResult.relatives.length shouldBe 0
      secondResult.crossRefs.length shouldBe 4
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
  }
}