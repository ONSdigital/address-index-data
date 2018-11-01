package uk.gov.ons.addressindex.writers

import org.apache.spark.rdd.RDD
import org.elasticsearch.spark._
import uk.gov.ons.addressindex.models.{HybridAddressEsDocument, HybridAddressSkinnyEsDocument}

/**
  * Contains methods that store supplied structures into ElasticSearch
  * These methods should contain side effects that store the info into
  * ElasticSearch without any additional business logic
  */
object ElasticSearchWriter {

  /**
   * Stores addresses (Hybrid PAF & NAG) into ElasticSearch
   * @param data `RDD` containing addresses
   */
  def saveHybridAddresses(index: String, data: RDD[HybridAddressEsDocument]): Unit = data.saveToEs(index)

  /**
    * Stores addresses (Hybrid PAF & NAG) into ElasticSearch
    * @param data `RDD` containing addresses
    */
  def saveSkinnyHybridAddresses(index: String, data: RDD[HybridAddressSkinnyEsDocument]): Unit = data.saveToEs(index)
}
