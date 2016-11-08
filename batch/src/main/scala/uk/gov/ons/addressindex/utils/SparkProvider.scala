package uk.gov.ons.addressindex.utils

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Provide global access to the spark context instance.
  * Also handles the initialization of the spark context
  */
object SparkProvider {
  private val config = ConfigFactory.load()

  private val appName = config.getString("addressindex.spark.app-name")
  private val master = config.getString("addressindex.spark.master")

  private val conf = new SparkConf().setAppName(appName).setMaster(master)
  conf.set("spark.serializer", config.getString("addressindex.spark.serializer"))

  conf.set("es.nodes", config.getString("addressindex.elasticsearch.nodes"))
  conf.set("es.port", config.getString("addressindex.elasticsearch.port"))
  conf.set("es.net.http.auth.user", config.getString("addressindex.elasticsearch.user"))
  conf.set("es.net.http.auth.pass", config.getString("addressindex.elasticsearch.pass"))

  // decides either if ES index should be created manually or not
  conf.set("es.index.auto.create", config.getString("addressindex.elasticsearch.index-autocreate"))
  // IMPORTANT: without this elasticsearch-hadoop will try to access the interlan nodes
  // that are located on a private ip address. This is generally the case when es is
  // located on a cloud behind a public ip. More: https://www.elastic.co/guide/en/elasticsearch/hadoop/master/cloud.html
  conf.set("es.nodes.wan.only", config.getString("addressindex.elasticsearch.wan-only"))

  private lazy val sparkContext = SparkContext.getOrCreate(conf)
  lazy val sqlContext = SQLContext.getOrCreate(sparkContext)
}