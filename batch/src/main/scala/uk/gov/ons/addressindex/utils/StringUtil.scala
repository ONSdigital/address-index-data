package uk.gov.ons.addressindex.utils

object StringUtil {

  // turns a String into a Some(s) if trimming it results in a non-empty string, or else None
  def strToOpt(str: String): Option[String] = {
    val trimmed = str.trim
    if (trimmed.isEmpty) None else Some(trimmed)
  }

  // apply title casing to words in the string
  def applyTitleCasing(input: String): String = input.trim.toLowerCase.split(" ").map(_.capitalize).mkString(" ")

}
