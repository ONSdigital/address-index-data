package uk.gov.ons.addressindex.utils

import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.config.ConfigFactory
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

  sqlContext.udf.register("concatNag", concatNag(_: String, _: String, _: String, _: String, _: String, _: String, _: String,
    _: String, _: String, _: String, _: String, _: String, _: String, _: String, _: String))

  val incrementalId = new AtomicInteger()

  def registerTempTable(dataFrame: DataFrame, name: String): String = {
    val generatedName = name + incrementalId.getAndIncrement()
    dataFrame.registerTempTable(generatedName)
    generatedName
  }


  def concatNag(saoStartNumber: String, saoEndNumber: String, saoEndSuffix: String, saoStartSuffix: String,
                saoText: String, organisation: String, paoStartNumber: String, paoStartSuffix: String,
                paoEndNumber: String, paoEndSuffix: String, paoText: String, streetDescriptor: String,
                townName: String, locality: String, postcodeLocator: String): String = {

    val saoLeftRangeExists = saoStartNumber.nonEmpty || saoStartSuffix.nonEmpty
    val saoRightRangeExists = saoEndNumber.nonEmpty || saoEndSuffix.nonEmpty
    val saoHyphen = if (saoLeftRangeExists && saoRightRangeExists) "-" else ""

    val saoNumbers = Seq(saoStartNumber, saoStartSuffix, saoHyphen, saoEndNumber, saoEndSuffix)
      .map(_.trim).mkString
    val sao =
      if (saoText == organisation || saoText.isEmpty) saoNumbers
      else if (saoNumbers.isEmpty) s"$saoText"
      else s"$saoNumbers $saoText"

    val paoLeftRangeExists = paoStartNumber.nonEmpty || paoStartSuffix.nonEmpty
    val paoRightRangeExists = paoEndNumber.nonEmpty || paoEndSuffix.nonEmpty
    val paoHyphen = if (paoLeftRangeExists && paoRightRangeExists) "-" else ""

    val paoNumbers = Seq(paoStartNumber, paoStartSuffix, paoHyphen, paoEndNumber, paoEndSuffix)
      .map(_.trim).mkString
    val pao =
      if (paoText == organisation || paoText.isEmpty) paoNumbers
      else if (paoNumbers.isEmpty) s"$paoText"
      else s"$paoText $paoNumbers"

    val trimmedStreetDescriptor = streetDescriptor.trim
    val buildingNumberWithStreetDescription =
      if (pao.isEmpty) s"$sao $trimmedStreetDescriptor"
      else if (sao.isEmpty) s"$pao $trimmedStreetDescriptor"
      else if (pao.isEmpty && sao.isEmpty) trimmedStreetDescriptor
      else s"$sao $pao $trimmedStreetDescriptor"

    Seq(organisation, buildingNumberWithStreetDescription, locality,
      townName, postcodeLocator).map(_.trim).filter(_.nonEmpty).mkString(" ")
  }
}
