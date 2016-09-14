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
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession, functions}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.hive.HiveGenericUDF
import org.apache.spark.sql.hive.HiveShim.HiveFunctionWrapper
import org.apache.spark.sql.types._
import org.apache.spark.test.TestUtils
import org.apache.spark.util.Benchmark

class MiscBenchmark extends SparkFunSuite {

  lazy val sparkSession = SparkSession.builder
    .master("local[1]")
    .appName("microbenchmark")
    .config("spark.sql.shuffle.partitions", 1)
    .config("spark.sql.codegen.wholeStage", true)
    .getOrCreate()

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
    val numIters = 3
    val benchmark = new Benchmark("sigmoid functions", N)
    val schema = StructType(StructField("value", DoubleType) :: Nil)
    val testDf = sparkSession.createDataFrame(
      sparkSession.range(N).map(_.toDouble).map(Row(_))(RowEncoder(schema)).rdd,
      schema
    )
    testDf.cache.count // Cached
    def addBenchmarkCase(name: String, df: DataFrame): Unit = {
      benchmark.addCase(name, numIters) { _ =>
        df.queryExecution.executedPlan(0).execute().foreach(x => Unit)
      }
    }
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
    def sigmoidHiveUdf(exprs: Column*): Column = {
      val udf = HiveGenericUDF("sigmoid",
        new HiveFunctionWrapper("hivemall.tools.math.SigmoidGenericUDF"),
        exprs.map(_.expr))
      Column(udf)
    }
    addBenchmarkCase(
      "hive-udf",
      testDf.select(sigmoidHiveUdf($"value"))
    )
    benchmark.run()
  }
}
