/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.examples

import org.apache.spark.rdd.MappedRDD

import scala.math.random

import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.spark.SparkContext._

/** Computes an approximation to pi */
object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Worldcount" + args(1))
    val spark = new SparkContext(conf)
    val fName = args(0)
    val file = spark.textFile(fName)

    if (args.length == 3) {
      val partition = args(2).toInt
      val counts = file.flatMap(line => line.split(" "))
        .map(word => (word, 1))
        .reduceByKey((a, b) => a + b, partition)
      counts.saveAsTextFile(args(1))
    } else {
      val counts = file.flatMap(line => line.split(" "))
        .map(word => (word, 1))
        .reduceByKey((a, b) => a + b)
      counts.saveAsTextFile(args(1))
    }

    spark.stop()
  }
}

