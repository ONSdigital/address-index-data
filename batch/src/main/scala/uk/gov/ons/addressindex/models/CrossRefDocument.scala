package uk.gov.ons.addressindex.models

import org.apache.spark.sql.Row

/**
  * DTO containing data about crossrefs (crossReference, source) of a particular address
  *
  * @param uprn      address's uprn
  * @param crossRefs Array of crossrefs
  */
case class CrossRefDocument(uprn: Long, crossRefs: Array[Map[String, String]])

object CrossRefDocument {

  /**
    * Transforms data gathered in the join into a crossref document
    * @param crossRef row from the initial crossref data
    * @param crossRefs corresponding crossrefs
    * @return crossref document relevant to an address
    */
  def fromJoinData(crossRef: Row, crossRefs: Iterable[Row]): CrossRefDocument = {
    val uprn = crossRef.getLong(3);
    val crossRefsMap = crossRefs.toArray.map {
      row =>
        Map(
          "crossReference" -> row.getString(1),
          "source" -> row.getString(2)
        )
    }
    CrossRefDocument(uprn, crossRefsMap)
  }
}
