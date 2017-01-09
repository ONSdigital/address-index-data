package uk.gov.ons.addressindex.writers

import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark._
import uk.gov.ons.addressindex.models.HybridAddressEsDocument

/**
  * Contains methods that store supplied structures into ElasticSearch
  * These methods should contain side effects that store the info into
  * ElasticSearch without any additional business logic
  */
object ElasticSearchWriter {

  private lazy val config = ConfigFactory.load()
  private lazy val pafIndex = config.getString("addressindex.elasticsearch.indices.paf")
  private lazy val nagIndex = config.getString("addressindex.elasticsearch.indices.nag")
  private lazy val hybridIndex = config.getString("addressindex.elasticsearch.indices.hybrid")

  /**
    * Stores addresses (PAF) into ElasticSearch
    * @param data `DataFrame` containing addresses
    */
  def savePAFAddresses(data:DataFrame): Unit = data.saveToEs(pafIndex)

  /**
    * Stores addresses (NAG) into ElasticSearch
    * @param data `DataFrame` containing addresses
    */
  def saveNAGAddresses(data:DataFrame): Unit = data.saveToEs(nagIndex)

  /**
   * Stores addresses (Hybrid PAF & NAG) into ElasticSearch
   * @param data `RDD` containing addresses
   */
  def saveHybridAddresses(data: RDD[HybridAddressEsDocument]): Unit = data.saveToEs(hybridIndex)
}
