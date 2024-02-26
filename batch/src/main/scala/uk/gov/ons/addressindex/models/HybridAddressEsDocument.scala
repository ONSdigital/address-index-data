package uk.gov.ons.addressindex.models

case class HybridAddressEsDocument(uprn: Long,
                                   postcodeIn: String,
                                   postcodeOut: String,
                                   parentUprn: Long,
                                   relatives: Seq[Map[String, Any]],
                                   lpi: Seq[Map[String, Any]],
                                   paf: Seq[Map[String, Any]],
                                   crossRefs: Seq[Map[String, String]],
                                   classificationCode: Option[String],
                                   postcode: String,
                                   fromSource: String,
                                   countryCode: String,
                                   postcodeStreetTown: String,
                                   postTown: String,
                                   mixedPartial: String,
                                   addressEntryId: Option[Long],
                                   addressEntryIdAlphanumericBackup: Option[String])

object HybridAddressEsDocument extends EsDocument with HybridAddress
