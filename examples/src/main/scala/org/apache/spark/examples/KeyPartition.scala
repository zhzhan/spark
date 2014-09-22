package org.apache.spark.examples

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
/**
 * Created by zzhang on 9/19/14.
 */

object KeyPartition {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Partition" + args(1))
    val spark = new SparkContext(conf)
    val fName = args(0)
    val textFile = spark.textFile(fName)
    val rec = textFile.map(a=>(a.split('|')(10), a))
    if (args.length == 3) {
      val recs = rec.groupByKey(args(2).toInt)
      recs.saveRecordAsTextFile(args(1))
    } else {
      val recs = rec.groupByKey()
      recs.saveRecordAsTextFile(args(1))
    }

    spark.stop()

  }
}
