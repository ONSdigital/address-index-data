package uk.gov.ons.addressindex.models

case class HybridAddressSkinnyEsDocument(uprn: Long,
                                         parentUprn: Long,
                                         lpi: Seq[Map[String, Any]],
                                         paf: Seq[Map[String, Any]],
                                         classificationCode: Option[String],
                                         censusAddressType: String,
                                         censusEstabType: String,
                                         postcode: String,
                                         fromSource: String,
                                         countryCode: String,
                                         postcodeStreetTown: String,
                                         postTown: String,
                                         mixedPartial: String,
                                         onsAddressId: Option[String])

object HybridAddressSkinnyEsDocument extends EsDocument with HybridAddressSkinny