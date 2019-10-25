package uk.gov.ons.addressindex.utils

import org.scalatest.{Matchers, WordSpec}

class AuthUtilSpec extends WordSpec with Matchers {

 val expectedAuthString = "aGFja21lOnBsZWFzZQ=="

  "AuthUtil" should {
    "return the correct Base 64 authorization string" in {

      // Given
      val username = "hackme"
      val password = "please"

      // When
      val result = AuthUtil.encodeCredentials(username, password)

      // Then
      result shouldBe expectedAuthString

    }
  }
}
