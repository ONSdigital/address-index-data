package uk.gov.ons.addressindex.utils

import org.apache.commons.codec.binary.Base64

object AuthUtil {

  def encodeCredentials(username: String, password: String): String = {
    Base64.encodeBase64String(s"$username:$password".getBytes)
  }

}
