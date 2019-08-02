package uk.gov.ons.addressindex.utils

object StringUtil {

  def strToOpt(str: String): Option[String] = if (str.isEmpty) None else Some(str)

}
