package uk.gov.ons.addressindex.writers

import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.elasticsearch.spark._
import uk.gov.ons.addressindex.models.{HybridAddressEsDocument, HybridAddressSkinnyEsDocument}

/**
  * Contains methods that store supplied structures into ElasticSearch
  * These methods should contain side effects that store the info into
  * ElasticSearch without any additional business logic
  */
object ElasticSearchWriter {

  val config = ConfigFactory.load()
  val username = config.getString("addressindex.elasticsearch.user")
  val password = config.getString("addressindex.elasticsearch.pass")

  val sparkConf: Map[String, String] = Map(
    "es.net.http.auth.user" -> username,
    "es.net.http.auth.pass" -> password
  )

  /**
    * Stores addresses (Hybrid PAF & NAG) into ElasticSearch
    *
    * @param data `RDD` containing addresses
    */
  def saveHybridAddresses(index: String, data: RDD[HybridAddressEsDocument]): Unit = data.saveToEs(index, sparkConf)


  /**
    * Stores addresses (Hybrid PAF & NAG) into ElasticSearch
    *
    * @param data `RDD` containing addresses
    */
  def saveSkinnyHybridAddresses(index: String, data: RDD[HybridAddressSkinnyEsDocument]): Unit = data.saveToEs(index, sparkConf)

}