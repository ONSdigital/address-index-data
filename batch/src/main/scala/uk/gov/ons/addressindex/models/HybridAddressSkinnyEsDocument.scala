package uk.gov.ons.addressindex.models

case class HybridAddressSkinnyEsDocument(uprn: Long,
                                         parentUprn: Long,
                                         lpi: Seq[Map[String, Any]],
                                         paf: Seq[Map[String, Any]],
                                         classificationCode: Option[String],
                                         postcode: String,
                                         fromSource: String,
                                         countryCode: String,
                                         postcodeStreetTown: String,
                                         postTown: String,
                                         mixedPartial: String,
                                         addressEntryId: Option[Long],
                                         addressEntryIdAlphanumericBackup: Option[String])

object HybridAddressSkinnyEsDocument extends EsDocument with HybridAddressSkinny