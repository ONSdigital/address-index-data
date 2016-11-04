package main

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Counter {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("AI Counter")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile("hdfs://dev3/ons/addressIndex/AddressBase/ABP_E39_DELIVERY_POINT.csv")

    println("Partitions: " + textFile.partitions.length)
    println("Total lines: " + textFile.count())
    println("First line: " + textFile.first())
    println("First 3 lines: " + textFile.take(3))

    val linesWithEmpson = textFile.filter(line => line.contains("EMPSON"))

    println("Lines with Empson Count: " + linesWithEmpson.count())
    linesWithEmpson.collect.foreach(println)

    linesWithEmpson.saveAsTextFile("hdfs://dev3/ons/addressIndex/Results/empson_results")
  }
}
