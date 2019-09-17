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

package org.apache.spark.sql.execution.benchmark

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Benchmark

import scala.collection.mutable

/**
 * Benchmark to measure TPCH query performance.
 * To run this:
 *  spark-submit --class <this class> <spark sql test jar> --data-location <TPCH data location>
 */
object TPCHQueryBenchmark extends Logging {

  val dfMap = mutable.HashMap.empty[String, DataFrame]

  private def setupTables(spark: SparkSession, tables: Seq[String],
                          dataLocation: String): Map[String, Long] = {

//    val N = 500 << 17
    val N = 59986052
    val range = spark.range(N).toDF()
//    val range = range0.withColumn("id", functions.lit(1))
//      .withColumn("l_shipdate", functions.lit("1986-09-14"))
    dfMap.put("range", range)
    range.createOrReplaceTempView("range")
    range.persist(StorageLevel.MEMORY_ONLY)

    val tableMap = tables.map { tableName =>
      val input = spark.read.parquet(s"$dataLocation/$tableName")
      dfMap.put(tableName, input)
      input.createOrReplaceTempView(tableName)
      input.persist(StorageLevel.MEMORY_ONLY)
      tableName -> 0L
//      tableName -> spark.table(tableName).count()
    }.toMap

    tableMap
  }

  private def runTpchQueries(spark: SparkSession,
                              queryLocation: String,
                              queries: Seq[String],
                              tableSizes: Map[String, Long],
                              numIters: Int,
                              nameSuffix: String = ""): Unit = {
    queries.foreach { name =>
      val queryString = resourceToString(s"$queryLocation/$name.sql",
        classLoader = Thread.currentThread().getContextClassLoader)

      // This is an indirect hack to estimate the size of each query's input by traversing the
      // logical plan and adding up the sizes of all tables that appear in the plan.
      val queryRelations = scala.collection.mutable.HashSet[String]()
      spark.sql(queryString).queryExecution.analyzed.foreach {
        case SubqueryAlias(alias, _: LogicalRelation) =>
          queryRelations.add(alias.identifier)
        case LogicalRelation(_, _, Some(catalogTable), _) =>
          queryRelations.add(catalogTable.identifier.table)
        case HiveTableRelation(tableMeta, _, _) =>
          queryRelations.add(tableMeta.identifier.table)
        case _ =>
      }
      val numRows = queryRelations.map(tableSizes.getOrElse(_, 0L)).sum
      val benchmark = new Benchmark(s"TPCH", numRows, numIters)

      queryRelations.foreach(spark.table(_).count()) // to materialize tables

      benchmark.addCase(s"$name$nameSuffix wholestage off") { _ =>
        spark.conf.set("spark.sql.codegen.wholeStage", value = false)
        spark.sql(queryString).collect().foreach(println)
      }
      benchmark.addCase(s"$name$nameSuffix wholestage on") { _ =>
        spark.conf.set("spark.sql.codegen.wholeStage", value = true)
        spark.sql(queryString).collect().foreach(println)
      }
      logInfo(s"\n\n===== TPCH QUERY BENCHMARK OUTPUT FOR $name =====\n")
      benchmark.run()
      logInfo(s"\n\n===== FINISHED $name =====\n")
    }

    {
      val name = "TPCH-Q1"
      val benchmark = new Benchmark(s"TPCH-Spark", 0, numIters)
      val query = new Q01()
      benchmark.addCase(s"$name$nameSuffix wholestage off") { _ =>
        spark.conf.set("spark.sql.codegen.wholeStage", value = false)
        query.execute(spark).collect().foreach(println)
      }
      benchmark.addCase(s"$name$nameSuffix wholestage on") { _ =>
        spark.conf.set("spark.sql.codegen.wholeStage", value = true)
        query.execute(spark).collect().foreach(println)
      }
      logInfo(s"\n\n===== TPCH QUERY BENCHMARK OUTPUT FOR $name =====\n")
      benchmark.run()
      logInfo(s"\n\n===== FINISHED $name =====\n")
    }

    {
      val name = "TPCH-Q1_simple-lineitem"
      val tableName = "lineitem"
      val benchmark = new Benchmark(s"TPCH-Spark", 0, numIters)
      val query = new Q01_simple()
      benchmark.addCase(s"$name$nameSuffix wholestage off") { _ =>
        spark.conf.set("spark.sql.codegen.wholeStage", value = false)
        query.execute(spark, tableName).collect().foreach(println)
      }
      benchmark.addCase(s"$name$nameSuffix wholestage on") { _ =>
        spark.conf.set("spark.sql.codegen.wholeStage", value = true)
        query.execute(spark, tableName).collect().foreach(println)
      }
      logInfo(s"\n\n===== TPCH QUERY BENCHMARK OUTPUT FOR $name =====\n")
      benchmark.run()
      logInfo(s"\n\n===== FINISHED $name =====\n")
    }

    {
      val name = "TPCH-Q1_simple2-lineitem"
      val tableName = "lineitem"
      val benchmark = new Benchmark(s"TPCH-Spark", 0, numIters)
      val query = new Q01_simple2()
      benchmark.addCase(s"$name$nameSuffix wholestage off") { _ =>
        spark.conf.set("spark.sql.codegen.wholeStage", value = false)
        query.execute(spark, tableName).collect().foreach(println)
      }
      benchmark.addCase(s"$name$nameSuffix wholestage on") { _ =>
        spark.conf.set("spark.sql.codegen.wholeStage", value = true)
        query.execute(spark, tableName).collect().foreach(println)
      }
      logInfo(s"\n\n===== TPCH QUERY BENCHMARK OUTPUT FOR $name =====\n")
      benchmark.run()
      logInfo(s"\n\n===== FINISHED $name =====\n")
    }

    {
      val lineitem = dfMap.get("lineitem").get
      val lineitem_id = lineitem.withColumn("id", functions.lit(1))
      lineitem_id.createOrReplaceTempView("lineitem_id")
      lineitem_id.persist(StorageLevel.MEMORY_ONLY)
      dfMap.put("lineitem_id", lineitem_id)

      val name = "TPCH-Q1_simple_groupbyid-lineitem_id"
      val tableName = "lineitem_id"
      val benchmark = new Benchmark(s"TPCH-Spark", 0, numIters)
      val query = new Q01_simple_groupbyid()
      benchmark.addCase(s"$name$nameSuffix wholestage off") { _ =>
        spark.conf.set("spark.sql.codegen.wholeStage", value = false)
        query.execute(spark, tableName).collect().foreach(println)
      }
      benchmark.addCase(s"$name$nameSuffix wholestage on") { _ =>
        spark.conf.set("spark.sql.codegen.wholeStage", value = true)
        query.execute(spark, tableName).collect().foreach(println)
      }
      logInfo(s"\n\n===== TPCH QUERY BENCHMARK OUTPUT FOR $name =====\n")
      benchmark.run()
      logInfo(s"\n\n===== FINISHED $name =====\n")

      lineitem_id.unpersist(true)
    }

    {
      val lineitem = dfMap.get("lineitem").get
      val lineitem_partial = lineitem.select("l_shipdate", "l_returnflag", "l_quantity", "l_orderkey")
      lineitem_partial.createOrReplaceTempView("lineitem_partial")
      lineitem_partial.persist(StorageLevel.MEMORY_ONLY)
      dfMap.put("lineitem_partial", lineitem_partial)

      val name = "TPCH-Q1_simple-lineitem_partial"
      val tableName = "lineitem_partial"
      val benchmark = new Benchmark(s"TPCH-Spark", 0, numIters)
      val query = new Q01_simple()
      benchmark.addCase(s"$name$nameSuffix wholestage off") { _ =>
        spark.conf.set("spark.sql.codegen.wholeStage", value = false)
        query.execute(spark, tableName).collect().foreach(println)
      }
      benchmark.addCase(s"$name$nameSuffix wholestage on") { _ =>
        spark.conf.set("spark.sql.codegen.wholeStage", value = true)
        query.execute(spark, tableName).collect().foreach(println)
      }
      logInfo(s"\n\n===== TPCH QUERY BENCHMARK OUTPUT FOR $name =====\n")
      benchmark.run()
      logInfo(s"\n\n===== FINISHED $name =====\n")

      lineitem_partial.unpersist(true)
    }

    {
      val name = "Range.filter.groupBy.sum"
      val benchmark = new Benchmark(s"Sample", 0, numIters)
      val query = new RangeFilter()
      benchmark.addCase(s"$name$nameSuffix wholestage off") { _ =>
        spark.conf.set("spark.sql.codegen.wholeStage", value = false)
        query.execute(spark).collect().foreach(println)
      }
      benchmark.addCase(s"$name$nameSuffix wholestage on") { _ =>
        spark.conf.set("spark.sql.codegen.wholeStage", value = true)
        query.execute(spark).collect().foreach(println)
      }
      logInfo(s"\n\n===== TPCH QUERY BENCHMARK OUTPUT FOR $name =====\n")
      benchmark.run()
      logInfo(s"\n\n===== FINISHED $name =====\n")
    }
  }

  private def filterQueries(
      origQueries: Seq[String],
      args: TPCHQueryBenchmarkArguments): Seq[String] = {
    if (args.queryFilter.nonEmpty) {
      origQueries.filter(args.queryFilter.contains)
    } else {
      origQueries
    }
  }

  def main(args: Array[String]): Unit = {
    val benchmarkArgs = new TPCHQueryBenchmarkArguments(args)

    // List of all TPC-H queries
    val tpchQueries = Seq(
      "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10", "q11",
      "q12", "q13", "q14", "q15", "q16", "q17", "q18", "q19", "q20",
      "q21", "q22")

    // If `--query-filter` defined, filters the queries that this option selects
    val queriesToRun = filterQueries(tpchQueries, benchmarkArgs)

    if (queriesToRun.isEmpty) {
      throw new RuntimeException(
        s"Empty queries to run. Bad query name filter: ${benchmarkArgs.queryFilter}")
    }

    val conf = new SparkConf().setAppName("TPC-H")

//    val conf = new SparkConf()
//      .setMaster("local[1]")
//      .setAppName("test-sql-context")
//      .set("spark.sql.parquet.compression.codec", "snappy")
//      .set("spark.sql.shuffle.partitions", "4")
//      .set("spark.driver.memory", "3g")
//      .set("spark.executor.memory", "3g")
//      .set("spark.sql.autoBroadcastJoinThreshold", (20 * 1024 * 1024).toString)
//      .set("spark.sql.crossJoin.enabled", "true")

    val spark = SparkSession.builder.config(conf).getOrCreate()

    val tables = Seq("customer", "lineitem",
      "nation", "region", "orders", "part", "partsupp", "supplier")

    val tableSizes = setupTables(spark, tables, benchmarkArgs.dataLocation)

    runTpchQueries(spark, queryLocation = "tpch", queries = queriesToRun, tableSizes,
      benchmarkArgs.numIter)
  }
}
