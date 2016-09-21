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

package org.apache.spark.sql.hive.benchmark

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{EachTopK, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{Generate, LogicalPlan, Project}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveGenericUDTF
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession, functions}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.hive.HiveGenericUDF
import org.apache.spark.sql.hive.HiveShim.HiveFunctionWrapper
import org.apache.spark.sql.types._
import org.apache.spark.test.TestUtils
import org.apache.spark.util.Benchmark

import org.apache.spark.sql.hive.HivemallUtils._

class TestFuncWrapper(df: DataFrame) {

  def each_top_k(k: Column, group: Column, value: Column, args: Column*)
    : DataFrame = withTypedPlan {
    val clusterDf = df.repartition(group).sortWithinPartitions(group)
    Generate(HiveGenericUDTF(
        "each_top_k",
        new HiveFunctionWrapper("hivemall.tools.EachTopKUDTF"),
        (Seq(k, group, value) ++ args).map(_.expr)),
      join = false, outer = false, None,
      (Seq("rank", "key") ++ args.map(_.named.name)).map(UnresolvedAttribute(_)),
      clusterDf.logicalPlan)
  }

  def each_top_k_improved(k: Int, group: String, score: String, args: String*)
    : DataFrame = withTypedPlan {
    val clusterDf = df.repartition(group).sortWithinPartitions(group)
    val childrenAttributes = clusterDf.logicalPlan.output
    val generator = Generate(
      EachTopK(
        k,
        clusterDf.resolve(group),
        clusterDf.resolve(score),
        childrenAttributes
      ),
      join = false, outer = false, None,
      (Seq("rank") ++ childrenAttributes.map(_.name)).map(UnresolvedAttribute(_)),
      clusterDf.logicalPlan)
    val attributes = generator.generatedSet
    val projectList = (Seq("rank") ++ args).map(s => attributes.find(_.name == s).get)
    Project(projectList, generator)
  }

  /**
   * A convenient function to wrap a logical plan and produce a DataFrame.
   */
  @inline private[this] def withTypedPlan(logicalPlan: => LogicalPlan): DataFrame = {
    val queryExecution = df.sparkSession.sessionState.executePlan(logicalPlan)
    val outputSchema = queryExecution.sparkPlan.schema
    new Dataset[Row](df.sparkSession, queryExecution, RowEncoder(outputSchema))
  }
}

object TestFuncWrapper {

  /**
   * Implicitly inject the [[TestFuncWrapper]] into [[DataFrame]].
   */
  implicit def dataFrameToTestFuncWrapper(df: DataFrame): TestFuncWrapper =
    new TestFuncWrapper(df)

  def sigmoid(exprs: Column*): Column = withExpr {
    HiveGenericUDF("sigmoid",
      new HiveFunctionWrapper("hivemall.tools.math.SigmoidGenericUDF"),
      exprs.map(_.expr))
  }

  /**
   * A convenient function to wrap an expression and produce a Column.
   */
  @inline private def withExpr(expr: Expression): Column = Column(expr)
}

class MiscBenchmark extends SparkFunSuite {

  lazy val sparkSession = SparkSession.builder
    .master("local[1]")
    .appName("microbenchmark")
    .config("spark.sql.shuffle.partitions", 1)
    .config("spark.sql.codegen.wholeStage", true)
    .getOrCreate()

  val numIters = 3

  private def addBenchmarkCase(name: String, df: DataFrame)(implicit benchmark: Benchmark): Unit = {
    benchmark.addCase(name, numIters) { _ =>
      df.queryExecution.executedPlan(0).execute().foreach(x => Unit)
    }
  }

  TestUtils.benchmark("closure/exprs/spark-udf/hive-udf") {
    /**
     * Java HotSpot(TM) 64-Bit Server VM 1.8.0_31-b13 on Mac OS X 10.10.2
     * Intel(R) Core(TM) i7-4578U CPU @ 3.00GHz
     *
     * sigmoid functions:       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
     * --------------------------------------------------------------------------------
     * exprs                         7708 / 8173          3.4         294.0       1.0X
     * closure                       7722 / 8342          3.4         294.6       1.0X
     * spark-udf                     7963 / 8350          3.3         303.8       1.0X
     * hive-udf                    13977 / 14050          1.9         533.2       0.6X
     */
    import sparkSession.sqlContext.implicits._
    val N = 100L << 18
    implicit val benchmark = new Benchmark("sigmoid", N)
    val schema = StructType(
      StructField("value", DoubleType) :: Nil
    )
    val testDf = sparkSession.createDataFrame(
      sparkSession.range(N).map(_.toDouble).map(Row(_))(RowEncoder(schema)).rdd,
      schema
    )
    testDf.cache.count // Cached

    def sigmoidExprs(expr: Column): Column = {
      val one: () => Literal = () => Literal.create(1.0, DoubleType)
      Column(one()) / (Column(one()) + functions.exp(-expr))
    }
    addBenchmarkCase(
      "exprs",
      testDf.select(sigmoidExprs($"value"))
    )

    addBenchmarkCase(
      "closure",
      testDf.map { d =>
        Row(1.0 / (1.0 + Math.exp(-d.getDouble(0))))
      }(RowEncoder(schema))
    )

    val sigmoidUdf = functions.udf((d: Double) => 1.0 / (1.0 + Math.exp(-d)))
    addBenchmarkCase(
      "spark-udf",
      testDf.select(sigmoidUdf($"value"))
    )
    addBenchmarkCase(
      "hive-udf",
      testDf.select(TestFuncWrapper.sigmoid($"value"))
    )

    benchmark.run()
  }

  TestUtils.benchmark("top-k query") {
    /**
     * top-k (k=100):          Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
     * -------------------------------------------------------------------------------
     * rank                       62748 / 62862          0.4        2393.6       1.0X
     * each_top_k (hive-udf)      41421 / 41736          0.6        1580.1       1.5X
     * each_top_k (exprs)         15793 / 16394          1.7         602.5       4.0X
     */
    import sparkSession.sqlContext.implicits._
    import TestFuncWrapper._
    val N = 100L << 18
    val topK = 100
    val numGroup = 3
    implicit val benchmark = new Benchmark(s"top-k (k=$topK)", N)
    val schema = StructType(
      StructField("key", IntegerType) ::
      StructField("score", DoubleType) ::
      StructField("value", StringType) ::
      Nil
    )
    val testDf = {
      val df = sparkSession.createDataFrame(
        sparkSession.sparkContext.range(0, N).map(_.toInt).map { d =>
          Row(d % numGroup, scala.util.Random.nextDouble(), s"group-${d % numGroup}")
        },
        schema
      )
      // Test data are clustered by group keys
      df.repartition($"key").sortWithinPartitions($"key")
    }
    testDf.cache.count // Cached

    addBenchmarkCase(
      "rank",
      testDf.withColumn(
        "rank", rank().over(Window.partitionBy($"key").orderBy($"score".desc))
      ).where($"rank" <= topK)
    )

    addBenchmarkCase(
      "each_top_k (hive-udf)",
      // TODO: If $"value" given, it throws `AnalysisException`. Why?
      // testDf.each_top_k(10, $"key", $"score", $"value")
      // org.apache.spark.sql.catalyst.analysis.UnresolvedException: Invalid call to name
      //   on unresolved object, tree: unresolvedalias('value, None)
      // at org.apache.spark.sql.catalyst.analysis.UnresolvedAlias.name(unresolved.scala:339)
      testDf.each_top_k(topK, $"key", $"score", testDf("value"))
    )

    addBenchmarkCase(
      "each_top_k (exprs)",
      testDf.each_top_k_improved(topK, "key", "score", "value")
    )

    benchmark.run()
  }
}
