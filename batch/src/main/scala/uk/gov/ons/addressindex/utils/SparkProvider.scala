package uk.gov.ons.addressindex.utils

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  */
object SparkProvider {
  private val appName = "ONS-address-base-batch"
  private val master = "local[*]"

  private val conf = new SparkConf().setAppName(appName).setMaster(master)

  private lazy val sparkContext = SparkContext.getOrCreate(conf)
  lazy val sqlContext = SQLContext.getOrCreate(sparkContext)
}
