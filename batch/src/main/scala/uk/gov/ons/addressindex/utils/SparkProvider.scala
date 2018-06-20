package uk.gov.ons.addressindex.utils

import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
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

  conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
  conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

  conf.set("spark.sql.shuffle.partitions", config.getString("addressindex.spark.sql.shuffle.partitions"))
  conf.set("spark.yarn.executor.memoryOverhead", config.getString("addressindex.spark.yarn.executor.memoryOverhead"))

  conf.set("es.nodes", config.getString("addressindex.elasticsearch.nodes"))
  conf.set("es.port", config.getString("addressindex.elasticsearch.port"))
//  conf.set("es.net.http.auth.user", config.getString("addressindex.elasticsearch.user"))
//  conf.set("es.net.http.auth.pass", config.getString("addressindex.elasticsearch.pass"))

  conf.set("es.batch.write.retry.count", config.getString("addressindex.elasticsearch.retry.count"))
  conf.set("es.batch.size.bytes", config.getString("addressindex.elasticsearch.batch.size.bytes"))
  conf.set("es.batch.size.entries", config.getString("addressindex.elasticsearch.batch.size.entries"))

  // decides either if ES index should be created manually or not
  conf.set("es.index.auto.create", config.getString("addressindex.elasticsearch.index-autocreate"))
  // IMPORTANT: without this elasticsearch-hadoop will try to access the interlan nodes
  // that are located on a private ip address. This is generally the case when es is
  // located on a cloud behind a public ip. More: https://www.elastic.co/guide/en/elasticsearch/hadoop/master/cloud.html
  conf.set("es.nodes.wan.only", config.getString("addressindex.elasticsearch.wan-only"))

  // this must fix duplication problem, hardcoded
  conf.set("es.mapping.id", "uprn")

  private lazy val sparkContext = SparkContext.getOrCreate(conf)
  lazy val sqlContext = new HiveContext(sparkContext)

  val incrementalId = new AtomicInteger()

  def registerTempTable(dataFrame: DataFrame, name: String): String = {
    val generatedName = name + incrementalId.getAndIncrement()
    dataFrame.registerTempTable(generatedName)
    generatedName
  }


}
