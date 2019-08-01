package uk.gov.ons.addressindex.models

import org.apache.spark.sql.Row

import scala.io.{BufferedSource, Source}
import scala.util.matching.Regex

abstract class EsDocument {

  def rowToLpi(row: Row): Map[String, Any]

  def rowToPaf(row: Row): Map[String, Any]

  val numberAndLetterRegex: Regex = "\\d+[A-Z]".r

  /**
    * Creates formatted address from PAF address
    * Adapted from API code
    *
    * @return String of formatted address
    */
  def generateFormattedPafAddress(poBoxNumber: String, buildingNumber: String, dependentThoroughfare: String,
                                  thoroughfare: String, departmentName: String, organisationName: String,
                                  subBuildingName: String, buildingName: String, doubleDependentLocality: String,
                                  dependentLocality: String, postTown: String, postcode: String): String = {

    // org, department, building name, thoroughfare, dependent locality, post town, postcode
    // org, department, po box, post town, postcode
    // building number + building name, post town, postcode

    def strToOpt(str: String): Option[String] = if (str.isEmpty) None else Some(str)

    def normalize(s: String): String = splitAndCapitalise(s)

    def normalizeTowns(s: String): String = splitAndCapitaliseTowns(s)

    val thoroughfares = Seq(dependentThoroughfare, thoroughfare).map(normalize).map(strToOpt)

    val premises = Seq(subBuildingName, buildingName, buildingNumber).map(normalize).map(strToOpt)

    val poBox = strToOpt(poBoxNumber).map("PO BOX " + _)

    // merge the first entry in thoroughfare, and the last entry in premises, if they exist
    val premsAndThoroughfare = (premises, thoroughfares) match {
      case (sb :: b :: Some(n) :: Nil, Some(t) :: ts) => sb :: b :: poBox :: Some(n + " " + t) :: ts
      case (sb :: Some(b) :: None :: Nil, Some(t) :: ts) => sb :: poBox :: Some(b + " " + t) :: ts
      case (ps, ts) => (ps :+ poBox) ++ ts
    }

    val org = Seq(departmentName, organisationName).map(normalize).map(strToOpt)
    val locality = Seq(doubleDependentLocality, dependentLocality, postTown).map(normalizeTowns).map(strToOpt)
    val postcodeOpt = Seq(strToOpt(postcode))
    (org ++ premsAndThoroughfare ++ locality ++ postcodeOpt).flatten.map(_.trim).mkString(", ")
  }

  /**
    * Creates Welsh formatted address from PAF address
    * Adapted from API code
    *
    * @return String of Welsh formatted address
    */
  def generateWelshFormattedPafAddress(poBoxNumber: String, buildingNumber: String, welshDependentThoroughfare: String,
                                       welshThoroughfare: String, departmentName: String, organisationName: String,
                                       subBuildingName: String, buildingName: String, welshDoubleDependentLocality: String,
                                       welshDependentLocality: String, welshPostTown: String, postcode: String): String = {

    generateFormattedPafAddress(poBoxNumber, buildingNumber, welshDependentThoroughfare,
      welshThoroughfare, departmentName, organisationName,
      subBuildingName, buildingName, welshDoubleDependentLocality,
      welshDependentLocality, welshPostTown, postcode)
  }

  /**
    * Formatted address should contain commas between all fields except after digits
    * The actual logic is pretty complex and should be treated on example-to-example level
    * (with unit tests)
    * Adapted from API code
    *
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

    Seq(splitAndCapitalise(organisation),
      buildingNumberWithStreetDescription,
      splitAndCapitaliseTowns(locality),
      splitAndCapitaliseTowns(townName),
      postcodeLocator
    ).map(_.trim).filter(_.nonEmpty).mkString(", ")
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

  // Used in splitAndCapitalise and splitAndCapitaliseTowns only
  val startsWithNumber: Regex = "^[0-9].*".r

  // check to see if the token is a listed acronym, if so skip capitilization
  // if it starts with a number, uppercase it
  def splitAndCapitalise(input: String): String = {
    input.trim.split(" ").map(it => {
      if (acronyms.contains(it)) it
      else if (startsWithNumber.findFirstIn(it).isDefined) it.toUpperCase
      else it.toLowerCase.capitalize
    }).mkString(" ")
  }

  // check to see if the token is a listed acronym, if so skip capitilization
  // next check to see of the token is on the list of hyphenated place, if so capitalise as per list
  // next check for parts in non-hyphenated names that are always lower case
  // if none of the above capitalize in the standard way
  def splitAndCapitaliseTowns(input: String): String = {
    input.trim.split(" ").map(it => {
      val hyphenMatch = hyphenplaces.get(it)
      val lowercaseMatch = lowercaseparts.get(it)
      if (acronyms.contains(it)) it
      else if (hyphenMatch.isDefined) hyphenMatch.get
      else if (lowercaseMatch.isDefined) lowercaseMatch.get
      else if (startsWithNumber.findFirstIn(it).isDefined) it.toUpperCase
      else it.toLowerCase.capitalize
    }).mkString(" ")
  }

  /**
    * List of acronyms to not capitalise
    */
  lazy val acronyms: Seq[String] = fileToList(s"acronyms")

  /**
    * List of placenames with hyphens
    */
  lazy val hyphenplaces: Map[String, String] = fileToMap(s"hyphenplaces", "=")

  /**
    * List of placenames with hyphens
    */
  lazy val lowercaseparts: Map[String, String] = fileToMap(s"lowercaseparts", "=")

  /**
    * Convert external file into list
    *
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
    * @param fileName  name of the file
    * @param delimiter optional, delimiter of values in the file, defaults to "="
    * @return Map containing key -> value from the file
    */
  def fileToMap(fileName: String, delimiter: String): Map[String, String] = {
    val resource = getResource(fileName)
    resource.getLines().map { l =>
      val Array(k, v1, _*) = l.split(delimiter)
      k -> v1
    }.toMap
  }

  /**
    * Fetch file stream as buffered source
    *
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
