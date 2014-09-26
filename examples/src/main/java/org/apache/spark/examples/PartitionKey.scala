package org.apache.spark.examples




import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark._
import org.apache.spark.SparkContext._

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

/**
 * Created by zzhang on 9/25/14.
 */
class MyMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  override def generateActualKey(key: Any, value: Any): Any = {
    NullWritable.get()
  }

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
    key.asInstanceOf[String]
  }
}

object PartitionBy {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Partition" + args(1))
    val spark = new SparkContext(conf)
    val fName = args(0)
    val textFile = spark.textFile(fName)
    val rec = textFile.map(a=>(a.split('|')(10), a))
    val par = rec.partitionBy(new HashPartitioner(args(2).toInt))
    par.saveAsHadoopFile(args(1), classOf[String], classOf[String], classOf[MyMultipleTextOutputFormat])
    spark.stop()
  }
}

object GroupBy {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Partition" + args(1))
    val spark = new SparkContext(conf)
    val fName = args(0)
    val textFile = spark.textFile(fName)
    textFile.flatMap(line => line.split(" "))
    val rec = textFile.map(a=>(a.split('|')(10), a))
    val recs = rec.groupByKey(args(2).toInt)
    val sin =  recs.flatMap(x=>x._2.map(m=>(x._1, m)))
    //  val sin = recs.flatMap(x=>x._2).map(a=>(a.split('|')(10), a))
    sin.saveAsHadoopFile(args(1), classOf[String], classOf[String], classOf[MyMultipleTextOutputFormat])
    spark.stop()

  }
}

object ReduceBy {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Partition" + args(1))
    val spark = new SparkContext(conf)
    val fName = args(0)
    val textFile = spark.textFile(fName)
    val rec = textFile.map(a=>(a.split('|')(10), Seq(a)))
    val red = rec.reduceByKey((x, y) => x ++ y, args(2).toInt)
    val sin =  red.flatMap(x=>x._2.map(m=>(x._1, m)))
    //    val sin = red.flatMap(x=>x._2).map(a=>(a.split('|')(10), a))
    sin.saveAsHadoopFile(args(1), classOf[String], classOf[String], classOf[MyMultipleTextOutputFormat])
    spark.stop()
  }
}