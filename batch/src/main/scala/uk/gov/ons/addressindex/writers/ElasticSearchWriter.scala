package uk.gov.ons.addressindex.writers

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.elasticsearch.spark.sql._

/**
  * Contains methods that store supplied structures into ElasticSearch
  * These methods should contain side effects that store the info into
  * ElasticSearch without any additional business logic
  */
object ElasticSearchWriter {

  private lazy val config = ConfigFactory.load()
  private lazy val PafIndex = config.getString("addressindex.elasticsearch.indices.paf")
  private lazy val NagIndex = config.getString("addressindex.elasticsearch.indices.nag")

  /**
    * Stores addresses (PAF) into ElasticSearch
    * @param data `DataFrame` containing addresses
    */
  def savePAFAddresses(data:DataFrame): Unit = data.saveToEs(PafIndex)

  /**
    * Stores addresses (NAG) into ElasticSearch
    * @param data `DataFrame` containing addresses
    */
  def saveNAGAddresses(data:DataFrame): Unit = data.saveToEs(NagIndex)
}
