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

import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.rdd.RDD

import scala.util.matching.Regex

object CaidaFlowAnalysis {
  val pattern: Regex = " *\\d+ +[0-9.]+ +([0-9.]+) -> ([0-9.]+) +.*Len=(\\d+)".r
  def mapper0(line: String): (String, (String, Long)) = {
    val matched = pattern.findFirstMatchIn(line).get
    return (matched.group(2), (matched.group(1), matched.group(3).toLong))
  }
  def mapper1(line: String): (String, (String, Long)) = {
    val matched = pattern.findFirstMatchIn(line).get
    return (matched.group(1), (matched.group(2), matched.group(3).toLong))
  }
  def stdev(data: Iterable[(String, (String, Long))]): Double = {
    var num: Long = 0
    var sum: Long = 0
    var squareSum: Long = 0
    data.map(x => x._2._2).foreach(x => {
      num += 1
      sum += x
      squareSum += (x * x)
    })
    if (num == 0) {
      return Double.NaN
    }
    val average: Double = sum.toDouble / num
    val squareAverage: Double = squareSum.toDouble / num
    return Math.sqrt(squareAverage - (average * average))
  }
  def getDouble(data: java.lang.Iterable[Double]): Double = {
    if (data.iterator().hasNext) {
      return data.iterator().next()
    }
    return Double.NaN
  }
  def main(args: Array[String]): Unit = {
    // args
    val inputPath0 = args(0)
    val inputPath1 = args(1)
    val outputPath = args(2)

    val sc = new SparkContext()
    val start = System.currentTimeMillis()

    val in0: RDD[(String, Double)] = sc.textFile(inputPath0)
      .filter(line => pattern.findFirstMatchIn(line).isDefined)
      .map(mapper0)
      .groupBy((x: (String, (String, Long)))=> x._1, 107)
      .map(x => (x._1, stdev(x._2)))

    val in1: RDD[(String, Double)] = sc.textFile(inputPath1)
      .filter(line => pattern.findFirstMatchIn(line).isDefined)
      .map(mapper1)
      .groupBy((x: (String, (String, Long)))=> x._1, 45)
      .map(x => (x._1, stdev(x._2)))


    val joined: RDD[(String, (java.lang.Iterable[Double], java.lang.Iterable[Double]))] = JavaPairRDD
      .fromRDD(in0).cogroup(JavaPairRDD.fromRDD(in1), 10).rdd
    val toWrite: RDD[String] = joined.map(x => x._1 + "," + getDouble(x._2._1).toString + "," + getDouble(x._2._2).toString)
    toWrite.saveAsTextFile(outputPath)

    // DONE
    println("*******END*******")
    println("JCT(ms): " + (System.currentTimeMillis()-start))
  }
}
