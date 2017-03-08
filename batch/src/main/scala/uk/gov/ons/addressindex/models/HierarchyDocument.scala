package uk.gov.ons.addressindex.models

import org.apache.spark.sql.Row

/**
  * DTO containing data about relatives (parent, children, siblings) of a particular address
  * @param uprn address's uprn
  * @param parentUprn address's parent uprn
  * @param relations Array of "levels"/"layers" of the hierarchy tree with siblings (on that level) and their parents
  */
case class HierarchyDocument(
  uprn: Long,
  parentUprn: Long,
  relations: Array[Map[String, Any]]
)

object HierarchyDocument {

  /**
    * Transforms data gathered in the join into a hierarchy document
    * @param hierarchy row from the initial hierarchy data
    * @param relations corresponding relations
    * @return hierarchy document relevant to an address
    */
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