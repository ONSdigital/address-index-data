package uk.gov.ons.addressindex.models

import org.apache.spark.sql.Row

/**
  *
  * @param uprn
  * @param parentUprn
  * @param relations
  */
case class HierarchyDocument(
  uprn: Long,
  parentUprn: Long,
  relations: Array[Map[String, Any]]
)

object HierarchyDocument {
  def fromJoinData(hierarchy: Row, relations: Iterable[Row]): HierarchyDocument = {
    val uprn = hierarchy.getLong(0)
    val parentUprn = if (hierarchy.isNullAt(5)) null.asInstanceOf[Long] else hierarchy.getLong(5)
    val relationsMap = relations.toArray.map { row =>
      Map(
        "level" -> row.getInt(1),
        "siblings" -> row.getAs[Array[Long]](2),
        "parents" -> row.getAs[Array[Long]](3)
      )
    }
    HierarchyDocument(uprn, parentUprn, relationsMap)
  }
}