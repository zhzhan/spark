package org.apache.spark.examples

import java.io.IOException
import java.text.{NumberFormat, SimpleDateFormat}
import java.util.Date

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.io.{Text, NullWritable}
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred.FileOutputCommitter
import org.apache.hadoop.mapred.FileOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.JobContext
import org.apache.hadoop.mapred.JobID
import org.apache.hadoop.mapred.OutputCommitter
import org.apache.hadoop.mapred.OutputFormat
import org.apache.hadoop.mapred.RecordWriter
import org.apache.hadoop.mapred.Reporter
import org.apache.hadoop.mapred.SparkHadoopMapRedUtil
import org.apache.hadoop.mapred.TaskAttemptContext
import org.apache.hadoop.mapred.TaskAttemptID
import org.apache.hadoop.mapred.TaskID
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.spark.HashPartitioner
import org.apache.spark.Logging
import org.apache.spark.SerializableWritable
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkException
import org.apache.spark.TaskContext
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.HadoopRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.{HadoopRDD, RDD}
import org.apache.hadoop.mapred._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.hadoop.io.compress.CompressionCodec
import java.util.{Date, HashMap => JHashMap}
import org.apache.hadoop.io.SequenceFile.CompressionType
import scala.collection.{Map, mutable}
import scala.collection.JavaConversions._

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
/**
 * Created by zzhang on 9/25/14.
 */
class MulOutputPerPartition {


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Partition" + args(1))
    val spark = new SparkContext(conf)
    val fName = args(0)
    val textFile = spark.textFile(fName)
    val rec = textFile.map(a=>(a.split('|')(10), a))
    val par = rec.partitionBy(new HashPartitioner(args(2).toInt))//.map(x => (NullWritable.get(), x))
    par.saveAsHadoopFile[MultipleTextOutputFormat[String, String]](args(1))

//  saveShuffledAsTextFile(args(1))
// spark.runJob()
spark.stop()

}
}