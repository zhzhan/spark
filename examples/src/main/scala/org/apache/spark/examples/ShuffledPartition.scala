package org.apache.spark.examples

import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * Created by zzhang on 9/23/14.
 */
object ShuffledPartition {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Partition" + args(1))
    val spark = new SparkContext(conf)
    val fName = args(0)
    val textFile = spark.textFile(fName)
    val rec = textFile.map(a=>(a.split('|')(10), a))
    val par = rec.partitionBy(new HashPartitioner(args(2).toInt))
    par.saveShuffledAsTextFile(args(1))
    spark.stop()

  }
}