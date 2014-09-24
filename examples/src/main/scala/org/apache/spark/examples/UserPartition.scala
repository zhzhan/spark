package org.apache.spark.examples

import java.io.IOException
import java.text.{NumberFormat, SimpleDateFormat}
import java.util.Date

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.io.NullWritable
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
import org.apache.hadoop.io.NullWritable
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
/**
 * Created by zzhang on 9/23/14.
 */







object UserPartition {
  def saveShuffledAsHadoopFile(rdd: RDD[_],
                               path: String,
                               keyClass: Class[_],
                               valueClass: Class[_],
                               outputFormatClass: Class[_ <: OutputFormat[_, _]],
                               conf: JobConf,
                               codec: Option[Class[_ <: CompressionCodec]] = None) {
    // Rename this as hadoopConf internally to avoid shadowing (see SPARK-2038).
    val hadoopConf = conf
    hadoopConf.setOutputKeyClass(keyClass)
    hadoopConf.setOutputValueClass(valueClass)
    // Doesn't work in Scala 2.9 due to what may be a generics bug
    // TODO: Should we uncomment this for Scala 2.10?
    // conf.setOutputFormat(outputFormatClass)
    hadoopConf.set("mapred.output.format.class", outputFormatClass.getName)
    for (c <- codec) {
      hadoopConf.setCompressMapOutput(true)
      hadoopConf.set("mapred.output.compress", "true")
      hadoopConf.setMapOutputCompressorClass(c)
      hadoopConf.set("mapred.output.compression.codec", c.getCanonicalName)
      hadoopConf.set("mapred.output.compression.type", CompressionType.BLOCK.toString)
    }
    hadoopConf.setOutputCommitter(classOf[FileOutputCommitter])
    FileOutputFormat.setOutputPath(hadoopConf,
      UserHadoopWriter.createPathFromString(path, hadoopConf))
    saveShuffledAsHadoopDataset(rdd, hadoopConf)
  }

  def saveShuffledAsHadoopDataset(rdd: RDD[_], conf: JobConf) {
    // Rename this as hadoopConf internally to avoid shadowing (see SPARK-2038).
    val hadoopConf = conf
    val outputFormatInstance = hadoopConf.getOutputFormat
    val keyClass = hadoopConf.getOutputKeyClass
    val valueClass = hadoopConf.getOutputValueClass
    if (outputFormatInstance == null) {
      throw new SparkException("Output format class not set")
    }
    if (keyClass == null) {
      throw new SparkException("Output key class not set")
    }
    if (valueClass == null) {
      throw new SparkException("Output value class not set")
    }
    SparkHadoopUtil.get.addCredentials(hadoopConf)

    if (rdd.conf.getBoolean("spark.hadoop.validateOutputSpecs", true)) {
      // FileOutputFormat ignores the filesystem parameter
      val ignoredFs = FileSystem.get(hadoopConf)
      hadoopConf.getOutputFormat.checkOutputSpecs(ignoredFs, hadoopConf)
    }

    val writer = new UserHadoopWriter(hadoopConf)
    writer.preSetup()

    val writeToFile = (context: TaskContext, iter: Iterator[_]) => {
      // Hadoop wants a 32-bit task attempt ID, so if ours is bigger than Int.MaxValue, roll it
      // around by taking a mod. We expect that no task will be attempted 2 billion times.
      val attemptNumber = (context.attemptId % Int.MaxValue).toInt

      writer.setup(context.stageId, context.partitionId, attemptNumber)
      var currentFiles: JHashMap[String, RecordWriter[AnyRef,AnyRef]] = new JHashMap[String, RecordWriter[AnyRef,AnyRef]]()
      var count = 0
      while (iter.hasNext) {
        val record = iter.next().asInstanceOf[(_, _)]
        val data = record._2.asInstanceOf[(String, Iterable[String])]
        try {
          var w = currentFiles.get(data._1)
          if (w == null) {
            w = writer.open(data._1)
            currentFiles.put(data._1, w)
          }
          w.write(record._1.asInstanceOf[AnyRef], data._2)
          count += 1
        }
      }
      currentFiles.values().foreach{w =>   w.close(Reporter.NULL)}
      writer.commit()
    }
    rdd.context.runJob(rdd, writeToFile)
    writer.commitJob()
  }


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Partition" + args(1))
    val spark = new SparkContext(conf)
    val fName = args(0)
    val textFile = spark.textFile(fName)
    val rec = textFile.map(a=>(a.split('|')(10), a))
    val par = rec.partitionBy(new HashPartitioner(args(2).toInt)).map(x => (NullWritable.get(), x))

    saveShuffledAsHadoopFile(par, args(1), classOf[NullWritable], classOf[String], classOf[TextOutputFormat[NullWritable, String]],
      new JobConf(spark.hadoopConfiguration))


    //  saveShuffledAsTextFile(args(1))
    // spark.runJob()
    spark.stop()

  }
}


class UserHadoopWriter(@transient jobConf: JobConf)
  extends Logging
  with SparkHadoopMapRedUtil
  with Serializable {

  private val now = new Date()
  private val conf = new SerializableWritable(jobConf)

  private var jobID = 0
  private var splitID = 0
  private var attemptID = 0
  private var jID: SerializableWritable[JobID] = null
  private var taID: SerializableWritable[TaskAttemptID] = null

  @transient private var writer: RecordWriter[AnyRef,AnyRef] = null
  @transient private var format: OutputFormat[AnyRef,AnyRef] = null
  @transient private var committer: OutputCommitter = null
  @transient private var jobContext: JobContext = null
  @transient private var taskContext: TaskAttemptContext = null

  def preSetup() {
    setIDs(0, 0, 0)
    HadoopRDD.addLocalConfiguration("", 0, 0, 0, conf.value)

    val jCtxt = getJobContext()
    getOutputCommitter().setupJob(jCtxt)
  }


  def setup(jobid: Int, splitid: Int, attemptid: Int) {
    setIDs(jobid, splitid, attemptid)
    HadoopRDD.addLocalConfiguration(new SimpleDateFormat("yyyyMMddHHmm").format(now),
      jobid, splitID, attemptID, conf.value)
  }

  def open() {
    val numfmt = NumberFormat.getInstance()
    numfmt.setMinimumIntegerDigits(5)
    numfmt.setGroupingUsed(false)

    val outputName = "part-"  + numfmt.format(splitID)
    val path = FileOutputFormat.getOutputPath(conf.value)
    val fs: FileSystem = {
      if (path != null) {
        path.getFileSystem(conf.value)
      } else {
        FileSystem.get(conf.value)
      }
    }

    getOutputCommitter().setupTask(getTaskContext())
    writer = getOutputFormat().getRecordWriter(fs, conf.value, outputName, Reporter.NULL)
  }
  def open(fName: String): RecordWriter[AnyRef,AnyRef] = {
    val numfmt = NumberFormat.getInstance()
    numfmt.setMinimumIntegerDigits(5)
    numfmt.setGroupingUsed(false)

    val outputName = fName
    val path = FileOutputFormat.getOutputPath(conf.value)
    val fs: FileSystem = {
      if (path != null) {
        path.getFileSystem(conf.value)
      } else {
        FileSystem.get(conf.value)
      }
    }

    getOutputCommitter().setupTask(getTaskContext())
    writer = getOutputFormat().getRecordWriter(fs, conf.value, outputName, Reporter.NULL)
    writer
  }
  def write(key: AnyRef, value: AnyRef) {
    if (writer != null) {
      writer.write(key, value)
    } else {
      throw new IOException("Writer is null, open() has not been called")
    }
  }

  def close() {
    writer.close(Reporter.NULL)
  }

  def commit() {
    val taCtxt = getTaskContext()
    val cmtr = getOutputCommitter()
    if (cmtr.needsTaskCommit(taCtxt)) {
      try {
        cmtr.commitTask(taCtxt)
        logInfo (taID + ": Committed")
      } catch {
        case e: IOException => {
          logError("Error committing the output of task: " + taID.value, e)
          cmtr.abortTask(taCtxt)
          throw e
        }
      }
    } else {
      logInfo ("No need to commit output of task: " + taID.value)
    }
  }

  def commitJob() {
    // always ? Or if cmtr.needsTaskCommit ?
    val cmtr = getOutputCommitter()
    cmtr.commitJob(getJobContext())
  }

  // ********* Private Functions *********

  private def getOutputFormat(): OutputFormat[AnyRef,AnyRef] = {
    if (format == null) {
      format = conf.value.getOutputFormat()
        .asInstanceOf[OutputFormat[AnyRef,AnyRef]]
    }
    format
  }

  private def getOutputCommitter(): OutputCommitter = {
    if (committer == null) {
      committer = conf.value.getOutputCommitter
    }
    committer
  }

  private def getJobContext(): JobContext = {
    if (jobContext == null) {
      jobContext = newJobContext(conf.value, jID.value)
    }
    jobContext
  }

  private def getTaskContext(): TaskAttemptContext = {
    if (taskContext == null) {
      taskContext =  newTaskAttemptContext(conf.value, taID.value)
    }
    taskContext
  }

  private def setIDs(jobid: Int, splitid: Int, attemptid: Int) {
    jobID = jobid
    splitID = splitid
    attemptID = attemptid

    jID = new SerializableWritable[JobID](UserHadoopWriter.createJobID(now, jobid))
    taID = new SerializableWritable[TaskAttemptID](
      new TaskAttemptID(new TaskID(jID.value, true, splitID), attemptID))
  }
}

object UserHadoopWriter {
  def createJobID(time: Date, id: Int): JobID = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    val jobtrackerID = formatter.format(time)
    new JobID(jobtrackerID, id)
  }

  def createPathFromString(path: String, conf: JobConf): Path = {
    if (path == null) {
      throw new IllegalArgumentException("Output path is null")
    }
    val outputPath = new Path(path)
    val fs = outputPath.getFileSystem(conf)
    if (outputPath == null || fs == null) {
      throw new IllegalArgumentException("Incorrectly formatted output path")
    }
    outputPath.makeQualified(fs)
  }
}