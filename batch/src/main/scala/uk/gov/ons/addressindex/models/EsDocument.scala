package uk.gov.ons.addressindex.models

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row
import uk.gov.ons.addressindex.utils.StringUtil.strToOpt

import scala.io.{BufferedSource, Source}
import scala.util.Try
import scala.util.matching.Regex
import scala.io.Codec
import java.nio.charset.CodingErrorAction

abstract class EsDocument {

  implicit val codec = Codec("UTF-8")
  codec.onMalformedInput(CodingErrorAction.REPLACE)
  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

  def rowToLpi(row: Row): Map[String, Any]

  def rowToPaf(row: Row): Map[String, Any]

  def generatePaf(poBoxNumber: String, buildingNumber: String,
                  dependentThoroughfare: String, thoroughfare: String,
                  departmentName: String, organisationName: String,
                  subBuildingName: String, buildingName: String,
                  doubleDependentLocality: String, dependentLocality: String,
                  postTown: String, postcode: String): Seq[String] = {
    val thoroughfares = Seq(dependentThoroughfare, thoroughfare).map(normalize).map(strToOpt)
    val premises = Seq(subBuildingName, buildingName, buildingNumber).map(normalize).map(strToOpt)
    val poBox = strToOpt(poBoxNumber).map("PO Box " + _)

    // merge the first entry in thoroughfare, and the last entry in premises, if they exist
    val premsAndThoroughfare = (premises, thoroughfares) match {
      case (sub_build :: build :: Some(number) :: Nil, Some(thorough_first) :: thorough_rest) =>
        sub_build :: build :: poBox :: Some(number + " " + thorough_first) :: thorough_rest
      case (sub_build :: build :: Some(number) :: Nil, None :: thorough_rest) =>
        sub_build :: build :: poBox :: Some(number + " " + thorough_rest.headOption.getOrElse(Some("")).getOrElse("")) :: Nil
      case (sub_build :: Some(build@startsWithNumber()) :: None :: Nil, Some(thorough_first) :: thorough_rest) =>
        sub_build :: poBox :: Some(build + " " + thorough_first) :: thorough_rest
      case (sub_build :: Some(build@startsWithNumber()) :: None :: Nil, None :: thorough_rest) =>
        sub_build :: poBox :: Some(build + " " + thorough_rest.headOption.getOrElse(Some("")).getOrElse("")) :: Nil
      case (premises, thoroughfares) => (premises :+ poBox) ++ thoroughfares
    }

    val org = Seq(departmentName, organisationName).map(normalize).map(strToOpt)
    val locality = Seq(doubleDependentLocality, dependentLocality, postTown).map(normalizeTowns).map(strToOpt)
    val postcodeOpt = Seq(strToOpt(postcode))
    (org ++ premsAndThoroughfare ++ locality ++ postcodeOpt).flatten
  }

  /**
    * Creates formatted address from PAF address
    * Adapted from API code
    *
    * @return String of formatted address
    */
  def generateFormattedPafAddress(poBoxNumber: String, buildingNumber: String,
                                  dependentThoroughfare: String, thoroughfare: String,
                                  departmentName: String, organisationName: String,
                                  subBuildingName: String, buildingName: String,
                                  doubleDependentLocality: String, dependentLocality: String,
                                  postTown: String, postcode: String): String = {
    generatePaf(
      poBoxNumber, buildingNumber,
      dependentThoroughfare, thoroughfare,
      departmentName, organisationName,
      subBuildingName, buildingName,
      doubleDependentLocality, dependentLocality,
      postTown, postcode
    ).mkString(", ")
  }

  /**
    * Creates Welsh formatted address from PAF address
    * Adapted from API code
    *
    * @return String of Welsh formatted address
    */
  def generateWelshFormattedPafAddress(poBoxNumber: String, buildingNumber: String,
                                       welshDependentThoroughfare: String, welshThoroughfare: String,
                                       departmentName: String, organisationName: String,
                                       subBuildingName: String, buildingName: String,
                                       welshDoubleDependentLocality: String, welshDependentLocality: String,
                                       welshPostTown: String,
                                       postcode: String): String = {
    generateFormattedPafAddress(poBoxNumber, buildingNumber, welshDependentThoroughfare,
      welshThoroughfare, departmentName, organisationName,
      subBuildingName, buildingName, welshDoubleDependentLocality,
      welshDependentLocality, welshPostTown, postcode)
  }

  def concatPaf(poBoxNumber: String, buildingNumber: String, dependentThoroughfare: String, welshDependentThoroughfare: String,
                thoroughfare: String, welshThoroughfare: String, departmentName: String, organisationName: String,
                subBuildingName: String, buildingName: String, doubleDependentLocality: String,
                welshDoubleDependentLocality: String, dependentLocality: String, welshDependentLocality: String,
                postTown: String, welshPostTown: String, postcode: String): String = {

    def unpair(eng: String, welsh: String): List[Option[String]] = {
      (strToOpt(eng), strToOpt(welsh)) match {
        case (Some(e), Some(w)) if e == w => List(Some(e))
        case (e, w) => List(e, w)
      }
    }

    ((departmentName :: organisationName ::
      subBuildingName :: buildingName ::
      poBoxNumber :: buildingNumber :: Nil
      ).map(strToOpt) :::
      unpair(dependentThoroughfare, welshDependentThoroughfare) :::
      unpair(thoroughfare, welshThoroughfare) :::
      unpair(doubleDependentLocality, welshDoubleDependentLocality) :::
      unpair(dependentLocality, welshDependentLocality) :::
      unpair(postTown, welshPostTown) :::
      strToOpt(postcode) :: Nil
      ).flatten.mkString(" ")
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
                                  streetDescriptor: String, locality: String, townName: String, postcodeLocator: String
                                 ): String = {

    val saoTextNormal = strToOpt(saoText).map(t => if (!t.contains("PO BOX")) normalize(t) else normalize(t))
    val saoNumbers = hyphenateNumbers(saoStartNumber, saoStartSuffix, saoEndNumber, saoEndSuffix).toUpperCase
    val sao = List(strToOpt(normalize(saoNumbers)), saoTextNormal.filter(_ != organisation))

    val paoNumbers = hyphenateNumbers(paoStartNumber, paoStartSuffix, paoEndNumber, paoEndSuffix).toUpperCase
    val paoNumbersAndStreet = List(paoNumbers, capitalizeFirst(normalizeTowns(streetDescriptor))).flatMap(strToOpt).mkString(" ")
    val pao = List(strToOpt(paoText).filter(_ != organisation).map(normalize), strToOpt(paoNumbersAndStreet))

    (strToOpt(normalize(organisation)) :: sao ::: pao :::
      strToOpt(normalizeTowns(locality)) :: strToOpt(normalizeTowns(townName)) :: strToOpt(postcodeLocator) :: Nil)
      .flatten.mkString(", ")
  }

  def capitalizeFirst(text: String): String = {
    if (text.isEmpty) "" else text.take(1).toUpperCase + text.drop(1)
  }

  def concatNag(saoStartNumber: String, saoEndNumber: String, saoEndSuffix: String, saoStartSuffix: String,
                saoText: String,
                organisation: String,
                paoStartNumber: String, paoStartSuffix: String, paoEndNumber: String, paoEndSuffix: String,
                paoText: String,
                streetDescriptor: String, townName: String, locality: String, postcodeLocator: String): String = {

    val saoNumbers = hyphenateNumbers(saoStartNumber, saoStartSuffix, saoEndNumber, saoEndSuffix).toUpperCase
    val sao = List(strToOpt(saoNumbers), strToOpt(saoText).filter(_ != organisation))

    val paoNumbers = hyphenateNumbers(paoStartNumber, paoStartSuffix, paoEndNumber, paoEndSuffix).toUpperCase
    val pao = List(strToOpt(paoText).filter(_ != organisation), strToOpt(paoNumbers))

    val data = strToOpt(organisation) :: sao ::: pao :::
      List(streetDescriptor, locality, townName, postcodeLocator).map(strToOpt)

    data.flatten.mkString(" ")
  }

  // Used in splitAndCapitalise and splitAndCapitaliseTowns only
  val startsWithNumber: Regex = "^[0-9].*".r

  // check to see if the token is a listed acronym, if so skip capitilization
  // if it starts with a number, uppercase it
  def normalize(input: String): String = {
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
  def normalizeTowns(input: String): String = {
    input.trim.split(" ").map(it => {
      val upper = it.toUpperCase
      val hyphenMatch = hyphenplaces.get(upper)
      val lowercaseMatch = lowercaseparts.get(upper)
      if (acronyms.contains(upper)) it
      else if (hyphenMatch.isDefined) hyphenMatch.get
      else if (lowercaseMatch.isDefined) lowercaseMatch.get
      else if (startsWithNumber.findFirstIn(it).isDefined) it.toUpperCase
      else it.toLowerCase.capitalize
    }).mkString(" ")
  }

  // add a hyphen between the start and end values if both exist
  // concatenate all values
  def hyphenateNumbers(startNumber: String, startSuffix: String, endNumber: String, endSuffix: String): String = {
    (startNumber.trim, startSuffix.trim, endNumber.trim, endSuffix.trim) match {
      case (sn, ss, "", "") => sn + ss
      case ("", "", en, es) => en + es
      case (sn, ss, en, es) => Seq(sn, ss, "-", en, es).mkString
    }
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
    * @param fileName name of the file
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
    * @param fileName name of the file
    * @return BufferedSource
    */
  def getResource(fileName: String): BufferedSource = {
    val path = "/" + fileName
    val currentDirectory = new java.io.File(".").getCanonicalPath
    // `Source.fromFile` needs an absolute path to the file, and current directory depends on where sbt was lauched
    // `getResource` may return null, that's why we wrap it into an `Option`
    Option(getClass.getResource(path)).map(Source.fromURL).getOrElse(Source.fromFile(currentDirectory + path))
  }

  def toShort(s: String): Option[Short] = {
    Try(s.toShort).toOption
  }

  def toInt(s: String): Option[Int] = {
    Try(s.toInt).toOption
  }

  def addLeadingZeros(in: String): String = {
    val tokens = StringUtils.trimToEmpty(in).split(" ")
    val newTokens = tokens.map{tok =>
    val ntok = tok.filter(_.isDigit)
    if (toInt(ntok).isDefined) tok.replace(ntok,StringUtils.leftPad(ntok,4,"0")) else tok
    }
    newTokens.mkString(" ")
  }


}
