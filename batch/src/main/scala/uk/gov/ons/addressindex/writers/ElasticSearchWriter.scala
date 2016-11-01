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
  private lazy val PAFIndex = config.getString("addressindex.es.paf")

  /**
    * Stores addresses (PAF) into ElasticSearch
    * @param data `DataFrame` containing addresses
    */
  def saveAddresses(data:DataFrame): Unit = data.saveToEs(PAFIndex)
}
