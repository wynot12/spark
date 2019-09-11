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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Benchmark

/**
 * Benchmark to measure TPCH query performance.
 * To run this:
 *  spark-submit --class <this class> <spark sql test jar> --data-location <TPCH data location>
 */
object TPCHQueryBenchmark extends Logging {

  private def setupTables(spark: SparkSession, tables: Seq[String],
                          dataLocation: String): Map[String, Long] = {
    tables.map { tableName =>
      val input = spark.read.parquet(s"$dataLocation/$tableName")
      input.createOrReplaceTempView(tableName)
      input.persist(StorageLevel.MEMORY_ONLY)
      tableName -> 0L
//      tableName -> spark.table(tableName).count()
    }.toMap
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
        spark.sql(queryString).collect()
      }
      benchmark.addCase(s"$name$nameSuffix wholestage on") { _ =>
        spark.conf.set("spark.sql.codegen.wholeStage", value = true)
        spark.sql(queryString).collect()
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
