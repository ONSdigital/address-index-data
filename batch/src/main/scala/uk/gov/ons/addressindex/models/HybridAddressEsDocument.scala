package uk.gov.ons.addressindex.models

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.Row

import scala.io.{BufferedSource, Source}

case class HybridAddressEsDocument(
  uprn: Long,
  postcodeIn: String,
  postcodeOut: String,
  parentUprn: Long,
  relatives: Seq[Map[String, Any]],
  lpi: Seq[Map[String, Any]],
  paf: Seq[Map[String, Any]],
  crossRefs: Seq[Map[String, String]],
  classifications: Seq[Map[String, String]]
)

object HybridAddressEsDocument {

  def rowToLpi(row: Row): Map[String, Any] = Map(
    "uprn" -> row.getLong(0),
    "postcodeLocator" -> row.getString(1),
    "addressBasePostal" -> row.getString(2),
    "location" -> row.get(3),
    "easting" -> row.getFloat(4),
    "northing" -> row.getFloat(5),
    "parentUprn" -> (if (row.isNullAt(6)) null else row.getLong(6)),
    "multiOccCount" -> row.getShort(7),
    "blpuLogicalStatus" -> row.getByte(8),
    "localCustodianCode" -> row.getShort(9),
    "rpc" -> row.getByte(10),
    "organisation" -> row.getString(11),
    "legalName" -> row.getString(12),
    "usrn" -> row.getInt(13),
    "lpiKey" -> row.getString(14),
    "paoText" -> row.getString(15),
    "paoStartNumber" -> (if (row.isNullAt(16)) null else row.getShort(16)),
    "paoStartSuffix" -> row.getString(17),
    "paoEndNumber" -> (if (row.isNullAt(18)) null else row.getShort(18)),
    "paoEndSuffix" -> row.getString(19),
    "saoText" -> row.getString(20),
    "saoStartNumber" -> (if (row.isNullAt(21)) null else row.getShort(21)),
    "saoStartSuffix" -> row.getString(22),
    "saoEndNumber" -> (if (row.isNullAt(23)) null else row.getShort(23)),
    "saoEndSuffix" -> row.getString(24),
    "level" -> row.getString(25),
    "officialFlag" -> row.getString(26),
    "lpiLogicalStatus" -> row.getByte(27),
    "usrnMatchIndicator" -> row.getByte(28),
    "language" -> row.getString(29),
    "streetDescriptor" -> row.getString(30),
    "townName" -> row.getString(31),
    "locality" -> row.getString(32),
    "streetClassification" -> (if (row.isNullAt(33)) null else row.getByte(33)),
    "lpiStartDate" -> row.getDate(34),
    "lpiLastUpdateDate" -> row.getDate(35),
    "lpiEndDate" -> row.getDate(36),
    "nagAll" ->  concatNag(
      if (row.isNullAt(21)) "" else row.getShort(21).toString,
      if (row.isNullAt(23)) "" else row.getShort(23).toString,
      row.getString(24), row.getString(22), row.getString(20), row.getString(11),
      if (row.isNullAt(16)) "" else row.getShort(16).toString,
      row.getString(17),
      if (row.isNullAt(18)) "" else row.getShort(18).toString,
      row.getString(19), row.getString(15), row.getString(30),
      row.getString(31), row.getString(32), row.getString(1)
    ),
    "mixedNag" -> generateFormattedNagAddress(
      if (row.isNullAt(21)) "" else row.getShort(21).toString,
      row.getString(22),
      if (row.isNullAt(23)) "" else row.getShort(23).toString,
      row.getString(24), row.getString(20), row.getString(11),
      if (row.isNullAt(16)) "" else row.getShort(16).toString,
      row.getString(17),
      if (row.isNullAt(18)) "" else row.getShort(18).toString,
      row.getString(19), row.getString(15), row.getString(30), row.getString(32), row.getString(31), row.getString(1)
    )
  )

  def rowToPaf(row: Row): Map[String, Any] = Map(
    "recordIdentifier" -> row.getByte(0),
    "changeType" -> Option(row.getString(1)).getOrElse(""),
    "proOrder" -> row.getLong(2),
    "uprn" -> row.getLong(3),
    "udprn" -> row.getInt(4),
    "organisationName" -> Option(row.getString(5)).getOrElse(""),
    "departmentName" -> Option(row.getString(6)).getOrElse(""),
    "subBuildingName" -> Option(row.getString(7)).getOrElse(""),
    "buildingName" -> Option(row.getString(8)).getOrElse(""),
    "buildingNumber" -> (if (row.isNullAt(9)) null else row.getShort(9)),
    "dependentThoroughfare" -> Option(row.getString(10)).getOrElse(""),
    "thoroughfare" -> Option(row.getString(11)).getOrElse(""),
    "doubleDependentLocality" -> Option(row.getString(12)).getOrElse(""),
    "dependentLocality" -> Option(row.getString(13)).getOrElse(""),
    "postTown" -> Option(row.getString(14)).getOrElse(""),
    "postcode" -> Option(row.getString(15)).getOrElse(""),
    "postcodeType" -> Option(row.getString(16)).getOrElse(""),
    "deliveryPointSuffix" -> Option(row.getString(17)).getOrElse(""),
    "welshDependentThoroughfare" -> Option(row.getString(18)).getOrElse(""),
    "welshThoroughfare" -> Option(row.getString(19)).getOrElse(""),
    "welshDoubleDependentLocality" -> Option(row.getString(20)).getOrElse(""),
    "welshDependentLocality" -> Option(row.getString(21)).getOrElse(""),
    "welshPostTown" -> Option(row.getString(22)).getOrElse(""),
    "poBoxNumber" -> Option(row.getString(23)).getOrElse(""),
    "processDate" -> row.getDate(24),
    "startDate" -> row.getDate(25),
    "endDate" -> row.getDate(26),
    "lastUpdateDate" ->row.getDate(27),
    "entryDate" -> row.getDate(28),
    "pafAll" -> concatPaf(Option(row.getString(23)).getOrElse(""),
      if (row.isNullAt(9)) "" else row.getShort(9).toString,
      Option(row.getString(10)).getOrElse(""),
      Option(row.getString(18)).getOrElse(""),
      Option(row.getString(11)).getOrElse(""),
      Option(row.getString(19)).getOrElse(""),
      Option(row.getString(6)).getOrElse(""),
      Option(row.getString(5)).getOrElse(""),
      Option(row.getString(7)).getOrElse(""),
      Option(row.getString(8)).getOrElse(""),
      Option(row.getString(12)).getOrElse(""),
      Option(row.getString(20)).getOrElse(""),
      Option(row.getString(13)).getOrElse(""),
      Option(row.getString(21)).getOrElse(""),
      Option(row.getString(14)).getOrElse(""),
      Option(row.getString(22)).getOrElse(""),
      Option(row.getString(15)).getOrElse("")),
    "mixedPaf" -> generateFormattedPafAddress(
      Option(row.getString(23)).getOrElse(""),
      if (row.isNullAt(9)) "" else row.getShort(9).toString,
      Option(row.getString(10)).getOrElse(""),
      Option(row.getString(11)).getOrElse(""),
      Option(row.getString(6)).getOrElse(""),
      Option(row.getString(5)).getOrElse(""),
      Option(row.getString(7)).getOrElse(""),
      Option(row.getString(8)).getOrElse(""),
      Option(row.getString(12)).getOrElse(""),
      Option(row.getString(13)).getOrElse(""),
      Option(row.getString(14)).getOrElse(""),
      Option(row.getString(15)).getOrElse("")
    ),
    "mixedWelshPaf" -> generateWelshFormattedPafAddress(
      Option(row.getString(23)).getOrElse(""),
      if (row.isNullAt(9)) "" else row.getShort(9).toString,
      Option(row.getString(18)).getOrElse(Option(row.getString(10)).getOrElse("")),
      Option(row.getString(19)).getOrElse(Option(row.getString(11)).getOrElse("")),
      Option(row.getString(6)).getOrElse(""),
      Option(row.getString(5)).getOrElse(""),
      Option(row.getString(7)).getOrElse(""),
      Option(row.getString(8)).getOrElse(""),
      Option(row.getString(20)).getOrElse(Option(row.getString(12)).getOrElse("")),
      Option(row.getString(21)).getOrElse(Option(row.getString(13)).getOrElse("")),
      Option(row.getString(22)).getOrElse(Option(row.getString(14)).getOrElse("")),
      Option(row.getString(15)).getOrElse("")
    )
  )

  def rowToHierarchy(row: Row): Map[String, Any] = Map(
    "level" -> row.getAs[String]("level"),
    "siblings" -> row.getAs[Array[Long]]("siblings"),
    "parents" -> row.getAs[Array[Long]]("parents")
  )

  def rowToCrossRef(row: Row): Map[String, String] = Map(
    "crossReference" -> row.getAs[String]("crossReference"),
    "source" -> row.getAs[String]("source")
  )

  def rowToClassification(row: Row): Map[String, String] = Map(
    "classScheme" -> row.getAs[String]("classScheme"),
    "classificationCode" -> row.getAs[String]("classificationCode")
  )

  // check to see if the token is a listed acronym, if so skip capitilization
  def splitAndCapitalise(input: String) : String = {
    input.trim.split(" ").map(
      {case y => if (acronyms.contains(y)) y
      else y.toLowerCase.capitalize}
    ).mkString(" ")
  }

  // check to see if the token is a listed acronym, if so skip capitilization
  // next check to see of the token is on the list of hyphenated place, if so capitalise as per list
  // next check for parts in non-hyphenated names that are always lower case
  // if noneof the above capitalize in the standard way
  def splitAndCapitaliseTowns(input: String) : String = {
    input.trim.split(" ").map(
      {case y => if (acronyms.contains(y)) y
      else if (!hyphenplaces.getOrElse(y,"").equals("")) hyphenplaces.getOrElse(y,"")
      else if (!lowercaseparts.getOrElse(y,"").equals("")) lowercaseparts.getOrElse(y,"")
      else y.toLowerCase.capitalize}
    ).mkString(" ")
  }

  /**
    * Creates formatted address from PAF address
    * Adapted from API code
    * @return String of formatted address
    */
  def generateFormattedPafAddress(poBoxNumber: String, buildingNumber: String, dependentThoroughfare: String,
                                  thoroughfare: String, departmentName: String, organisationName: String,
                                  subBuildingName: String, buildingName: String, doubleDependentLocality: String,
                                  dependentLocality: String, postTown: String, postcode: String): String = {

    val poBoxNumberEdit = if (poBoxNumber.isEmpty) "" else s"PO BOX ${poBoxNumber}"
    val numberAndLetter = "\\d+[A-Z]".r

    val trimmedBuildingNumber = buildingNumber.trim
    val trimmedDependentThoroughfare = splitAndCapitalise(dependentThoroughfare)
    val trimmedThoroughfare = splitAndCapitalise(thoroughfare)

    val buildingNumberWithStreetName =
      s"$trimmedBuildingNumber ${ if(trimmedDependentThoroughfare.nonEmpty) s"$trimmedDependentThoroughfare, " else "" }$trimmedThoroughfare"

    val departmentNameEdit = splitAndCapitalise(departmentName)
    val organisationNameEdit = splitAndCapitalise(organisationName)
    val subBuildingNameEdit = splitAndCapitalise(subBuildingName)
    val buildingNameEdit = if (numberAndLetter.findFirstIn(buildingName.trim).isEmpty) splitAndCapitalise(buildingName) else buildingName
    val doubleDependentLocalityEdit = splitAndCapitaliseTowns(doubleDependentLocality)
    val dependentLocalityEdit = splitAndCapitaliseTowns(dependentLocality)
    val postTownEdit = splitAndCapitaliseTowns(postTown)

    Seq(departmentNameEdit, organisationNameEdit, subBuildingNameEdit, buildingNameEdit,
      poBoxNumberEdit, buildingNumberWithStreetName, doubleDependentLocalityEdit, dependentLocalityEdit,
      postTownEdit, postcode).map(_.trim).filter(_.nonEmpty).mkString(", ")
  }

  /**
    * Creates Welsh formatted address from PAF address
    * Adapted from API code
    * @return String of Welsh formatted address
    */
  def generateWelshFormattedPafAddress(poBoxNumber: String, buildingNumber: String, welshDependentThoroughfare: String,
                                       welshThoroughfare: String, departmentName: String, organisationName: String,
                                       subBuildingName: String, buildingName: String, welshDoubleDependentLocality: String,
                                       welshDependentLocality: String, welshPostTown: String, postcode: String): String = {

    val poBoxNumberEdit = if (poBoxNumber.isEmpty) "" else s"PO BOX ${poBoxNumber}"
    val numberAndLetter = "\\d+[A-Z]".r

    val trimmedBuildingNumber = buildingNumber.trim
    val trimmedDependentThoroughfare = splitAndCapitalise(welshDependentThoroughfare)
    val trimmedThoroughfare = splitAndCapitalise(welshThoroughfare)

    val buildingNumberWithStreetName =
      s"$trimmedBuildingNumber ${ if(trimmedDependentThoroughfare.nonEmpty) s"$trimmedDependentThoroughfare, " else "" }$trimmedThoroughfare"

    val departmentNameEdit = splitAndCapitalise(departmentName)
    val organisationNameEdit = splitAndCapitalise(organisationName)
    val subBuildingNameEdit = splitAndCapitalise(subBuildingName)
    val buildingNameEdit = if (numberAndLetter.findFirstIn(buildingName.trim).isEmpty) splitAndCapitalise(buildingName) else buildingName
    val welshDoubleDependentLocalityEdit = splitAndCapitaliseTowns(welshDoubleDependentLocality)
    val welshDependentLocalityEdit = splitAndCapitaliseTowns(welshDependentLocality)
    val welshPostTownEdit = splitAndCapitaliseTowns(welshPostTown)

    Seq(departmentNameEdit, organisationNameEdit, subBuildingNameEdit, buildingNameEdit,
      poBoxNumberEdit, buildingNumberWithStreetName, welshDoubleDependentLocalityEdit, welshDependentLocalityEdit,
      welshPostTownEdit, postcode).map(_.trim).filter(_.nonEmpty).mkString(", ")
  }

  /**
    * Formatted address should contain commas between all fields except after digits
    * The actual logic is pretty complex and should be treated on example-to-example level
    * (with unit tests)
    * Adapted from API code
    * @return String of formatted address
    */
  def generateFormattedNagAddress(saoStartNumber: String, saoStartSuffix: String, saoEndNumber: String,
                               saoEndSuffix: String, saoText: String, organisation: String, paoStartNumber: String,
                               paoStartSuffix: String, paoEndNumber: String, paoEndSuffix: String, paoText: String,
                               streetDescriptor: String, locality: String, townName: String, postcodeLocator: String): String = {

    val saoLeftRangeExists = saoStartNumber.nonEmpty || saoStartSuffix.nonEmpty
    val saoRightRangeExists = saoEndNumber.nonEmpty || saoEndSuffix.nonEmpty
    val saoHyphen = if (saoLeftRangeExists && saoRightRangeExists) "-" else ""
    val saoNumbers = Seq(saoStartNumber, saoStartSuffix, saoHyphen, saoEndNumber, saoEndSuffix)
      .map(_.trim).mkString

    val sao =
      if (saoText == organisation || saoText.isEmpty) saoNumbers
      else if (saoText.contains("PO BOX")) if (saoNumbers.isEmpty) s"$saoText," else s"$saoNumbers, $saoText," // e.g. EX2 5ZX
      else if (saoNumbers.isEmpty) s"${splitAndCapitalise(saoText)},"
      else s"$saoNumbers, ${splitAndCapitalise(saoText)},"

    val paoLeftRangeExists = paoStartNumber.nonEmpty || paoStartSuffix.nonEmpty
    val paoRightRangeExists = paoEndNumber.nonEmpty || paoEndSuffix.nonEmpty
    val paoHyphen = if (paoLeftRangeExists && paoRightRangeExists) "-" else ""
    val paoNumbers = Seq(paoStartNumber, paoStartSuffix, paoHyphen, paoEndNumber, paoEndSuffix)
      .map(_.trim).mkString
    val pao =
      if (paoText == organisation || paoText.isEmpty) paoNumbers
      else if (paoNumbers.isEmpty) s"${splitAndCapitalise(paoText)},"
      else s"${splitAndCapitalise(paoText)}, $paoNumbers"

    val trimmedStreetDescriptor = splitAndCapitalise(streetDescriptor)
    val buildingNumberWithStreetDescription =
      if (pao.isEmpty) s"$sao $trimmedStreetDescriptor"
      else if (sao.isEmpty) s"$pao $trimmedStreetDescriptor"
      else if (pao.isEmpty && sao.isEmpty) trimmedStreetDescriptor
      else s"$sao $pao $trimmedStreetDescriptor"

    Seq(splitAndCapitalise(organisation), buildingNumberWithStreetDescription, splitAndCapitaliseTowns(locality),
      splitAndCapitaliseTowns(townName), postcodeLocator).map(_.trim).filter(_.nonEmpty).mkString(", ")
  }

  def concatPaf(poBoxNumber: String, buildingNumber: String, dependentThoroughfare: String, welshDependentThoroughfare:
  String, thoroughfare: String, welshThoroughfare: String, departmentName: String, organisationName: String,
    subBuildingName: String, buildingName: String, doubleDependentLocality: String,
    welshDoubleDependentLocality: String, dependentLocality: String, welshDependentLocality: String,
    postTown: String, welshPostTown: String, postcode: String): String = {

    val langDependentThoroughfare = if (dependentThoroughfare == welshDependentThoroughfare)
      s"$dependentThoroughfare" else s"$dependentThoroughfare $welshDependentThoroughfare"

    val langThoroughfare = if (thoroughfare == welshThoroughfare)
      s"$thoroughfare" else s"$thoroughfare $welshThoroughfare"

    val langDoubleDependentLocality = if (doubleDependentLocality == welshDoubleDependentLocality)
      s"$doubleDependentLocality" else s"$doubleDependentLocality $welshDoubleDependentLocality"

    val langDependentLocality = if (dependentLocality == welshDependentLocality)
      s"$dependentLocality" else s"$dependentLocality $welshDependentLocality"

    val langPostTown = if (postTown == welshPostTown)
      s"$postTown" else s"$postTown $welshPostTown"

    val buildingNumberWithStreetName =
      s"$buildingNumber ${if (langDependentThoroughfare.nonEmpty) s"$langDependentThoroughfare " else ""}$langThoroughfare"

    Seq(departmentName, organisationName, subBuildingName, buildingName,
      poBoxNumber, buildingNumberWithStreetName, langDoubleDependentLocality, langDependentLocality,
      langPostTown, postcode).map(_.trim).filter(_.nonEmpty).mkString(" ")
  }

  def concatNag(saoStartNumber: String, saoEndNumber: String, saoEndSuffix: String, saoStartSuffix: String,
    saoText: String, organisation: String, paoStartNumber: String, paoStartSuffix: String,
    paoEndNumber: String, paoEndSuffix: String, paoText: String, streetDescriptor: String,
    townName: String, locality: String, postcodeLocator: String): String = {

    val saoLeftRangeExists = saoStartNumber.nonEmpty || saoStartSuffix.nonEmpty
    val saoRightRangeExists = saoEndNumber.nonEmpty || saoEndSuffix.nonEmpty
    val saoHyphen = if (saoLeftRangeExists && saoRightRangeExists) "-" else ""

    val saoNumbers = Seq(saoStartNumber, saoStartSuffix, saoHyphen, saoEndNumber, saoEndSuffix)
      .map(_.trim).mkString
    val sao =
      if (saoText == organisation || saoText.isEmpty) saoNumbers
      else if (saoNumbers.isEmpty) s"$saoText"
      else s"$saoNumbers $saoText"

    val paoLeftRangeExists = paoStartNumber.nonEmpty || paoStartSuffix.nonEmpty
    val paoRightRangeExists = paoEndNumber.nonEmpty || paoEndSuffix.nonEmpty
    val paoHyphen = if (paoLeftRangeExists && paoRightRangeExists) "-" else ""

    val paoNumbers = Seq(paoStartNumber, paoStartSuffix, paoHyphen, paoEndNumber, paoEndSuffix)
      .map(_.trim).mkString
    val pao =
      if (paoText == organisation || paoText.isEmpty) paoNumbers
      else if (paoNumbers.isEmpty) s"$paoText"
      else s"$paoText $paoNumbers"

    val trimmedStreetDescriptor = streetDescriptor.trim
    val buildingNumberWithStreetDescription =
      if (pao.isEmpty) s"$sao $trimmedStreetDescriptor"
      else if (sao.isEmpty) s"$pao $trimmedStreetDescriptor"
      else if (pao.isEmpty && sao.isEmpty) trimmedStreetDescriptor
      else s"$sao $pao $trimmedStreetDescriptor"

    Seq(organisation, buildingNumberWithStreetDescription, locality,
      townName, postcodeLocator).map(_.trim).filter(_.nonEmpty).mkString(" ")
  }

  /**
    * List of acronyms to not capitalise
    */
  lazy val acronyms: Seq[String] = fileToList(s"acronyms")

  /**
    * List of placenames with hyphens
    */
  lazy val hyphenplaces: Map[String,String] = fileToMap(s"hyphenplaces","=")

  /**
    * List of placenames with hyphens
    */
  lazy val lowercaseparts: Map[String,String] = fileToMap(s"lowercaseparts","=")

  /**
    * Convert external file into list
    * @param fileName
    * @return
    */
  private def fileToList(fileName: String): Seq[String] = {
    val resource = getResource(fileName)
    resource.getLines().toList
  }

  /**
    * Make external file such as score matrix file into Map
    *
    * @param fileName name of the file
    * @param delimiter optional, delimiter of values in the file, defaults to "="
    * @return Map containing key -> value from the file
    */
  def fileToMap(fileName: String, delimiter: String ): Map[String,String] = {
    val resource = getResource(fileName)
    resource.getLines().map { l =>
      val Array(k,v1,_*) = l.split(delimiter)
      k -> v1
    }.toMap
  }

  /**
    * Fetch file stream as buffered source
    * @param fileName
    * @return
    */
  def getResource(fileName: String): BufferedSource = {
    val path = "/" + fileName
    val currentDirectory = new java.io.File(".").getCanonicalPath
    // `Source.fromFile` needs an absolute path to the file, and current directory depends on where sbt was lauched
    // `getResource` may return null, that's why we wrap it into an `Option`
    Option(getClass.getResource(path)).map(Source.fromURL).getOrElse(Source.fromFile(currentDirectory + path))
  }

}
