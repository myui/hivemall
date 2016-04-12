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

package org.apache.spark.test

import scala.collection.mutable.Seq

import org.apache.spark.sql.hive.HivemallOps
import org.apache.spark.sql.hive.HivemallOps._
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.types._
import org.apache.spark.sql.{QueryTest, functions, Row, SQLContext}
import org.apache.spark.test.TestUtils._

import hivemall.tools.RegressionDatagen

/**
 * Create a new [[SQLContext]] running in local-cluster mode.
 */
class HivemallQueryTest extends QueryTest with TestHiveSingleton {

  import hiveContext.implicits._

  protected val DummyInputData = {
    val rowRdd = hiveContext.sparkContext.parallelize(
        (0 until 4).map(Row(_))
      )
    val df = hiveContext.createDataFrame(
      rowRdd,
      StructType(
        StructField("data", IntegerType, true) ::
        Nil)
      )
    df
  }

  protected val IntList2Data = {
    val rowRdd = hiveContext.sparkContext.parallelize(
        Row(8 :: 5 :: Nil, 6 :: 4 :: Nil) ::
        Row(3 :: 1 :: Nil, 3 :: 2 :: Nil) ::
        Row(2 :: Nil, 3 :: Nil) ::
        Nil
      )
    hiveContext.createDataFrame(
      rowRdd,
      StructType(
        StructField("target", ArrayType(IntegerType), true) ::
        StructField("predict", ArrayType(IntegerType), true) ::
        Nil)
      )
  }

  protected val Float2Data = {
    val rowRdd = hiveContext.sparkContext.parallelize(
        Row(0.8f, 0.3f) ::
        Row(0.3f, 0.9f) ::
        Row(0.2f, 0.4f) ::
        Nil
      )
    hiveContext.createDataFrame(
      rowRdd,
      StructType(
        StructField("predict", DoubleType, true) ::
        StructField("target", DoubleType, true) ::
        Nil)
      )
  }

  protected val TinyTrainData = {
    val rowRdd = hiveContext.sparkContext.parallelize(
        Row(0.0, "1:0.8" :: "2:0.2" :: Nil) ::
        Row(1.0, "2:0.7" :: Nil) ::
        Row(0.0, "1:0.9" :: Nil) ::
        Nil
      )
    val df = hiveContext.createDataFrame(
      rowRdd,
      StructType(
        StructField("label", DoubleType, true) ::
        StructField("features", ArrayType(StringType), true) ::
        Nil)
      )
    df
  }

  protected val TinyTestData = {
    val rowRdd = hiveContext.sparkContext.parallelize(
        Row(0.0, "1:0.6" :: "2:0.1" :: Nil) ::
        Row(1.0, "2:0.9" :: Nil) ::
        Row(0.0, "1:0.2" :: Nil) ::
        Row(0.0, "2:0.1" :: Nil) ::
        Row(0.0, "0:0.6" :: "2:0.4" :: Nil) ::
        Nil
      )
    val df = hiveContext.createDataFrame(
      rowRdd,
      StructType(
        StructField("label", DoubleType, true) ::
        StructField("features", ArrayType(StringType), true) ::
        Nil)
      )
    df
  }

  protected val TinyScoreData = {
    val rowRdd = hiveContext.sparkContext.parallelize(
        Row(0.8f) :: Row(-0.3f) :: Row(0.2f) ::
        Nil
      )
    val df = hiveContext.createDataFrame(
      rowRdd,
      StructType(
        StructField("score", FloatType, true) ::
        Nil)
      )
    df
  }

  protected def checkRegrPrecision(func: String): Unit = {
    // Build a model
    val model = {
      val res = invokeFunc(new HivemallOps(LargeRegrTrainData),
        func, Seq(add_bias($"features"), $"label"))
      if (!res.columns.contains("conv")) {
        res.groupby("feature").agg("weight"->"avg")
      } else {
        res.groupby("feature").argmin_kld("weight", "conv")
      }
    }.as("feature", "weight")

    // Data preparation
    val testDf = LargeRegrTrainData
      .select(rowid(), $"label".as("target"), $"features")
      .cache

    val testDf_exploded = testDf
      .explode_array($"features")
      .select($"rowid", extract_feature($"feature"), extract_weight($"feature"))

    // Do prediction
    val predict = testDf_exploded
      .join(model, testDf_exploded("feature") === model("feature"), "LEFT_OUTER")
      .select($"rowid", ($"weight" * $"value").as("value"))
      .groupby("rowid").sum("value")
      .as("rowid", "predicted")

    // Evaluation
    val eval = predict
      .join(testDf, predict("rowid") === testDf("rowid"))
      .groupby()
      .agg(Map("target"->"avg", "predicted"->"avg"))
      .as("target", "predicted")

    val diff = eval.map {
      case Row(target: Double, predicted: Double) =>
        Math.abs(target - predicted)
    }.first

    TestUtils.expectResult(diff > 0.10,
      s"Low precision -> func:${func} diff:${diff}")
  }

  protected def checkClassifierPrecision(func: String): Unit = {
    // Build a model
    val model = {
      val res = invokeFunc(new HivemallOps(LargeClassifierTrainData),
        func, Seq(add_bias($"features"), $"label"))
      if (!res.columns.contains("conv")) {
        res.groupby("feature").agg("weight"->"avg")
      } else {
        res.groupby("feature").argmin_kld("weight", "conv")
      }
    }.as("feature", "weight")

    // Data preparation
    val testDf = LargeClassifierTestData
      .select(rowid(), $"label".as("target"), $"features")
      .cache

    val testDf_exploded = testDf
      .explode_array($"features")
      .select($"rowid", extract_feature($"feature"), extract_weight($"feature"))

    // Do prediction
    val predict = testDf_exploded
      .join(model, testDf_exploded("feature") === model("feature"), "LEFT_OUTER")
      .select($"rowid", ($"weight" * $"value").as("value"))
      .groupby("rowid").sum("value")
      /**
       * TODO: This sentence throws an exception below:
       *
       * WARN Column: Constructing trivially true equals predicate, 'rowid#1323 = rowid#1323'.
       * Perhaps you need to use aliases.
       */
      // .select($"rowid", functions.when(sigmoid($"sum(value)") > 0.50, 1.0).otherwise(0.0).as("predicted"))
      .select($"rowid", functions.when(sigmoid($"sum(value)") > 0.50, 1.0).otherwise(0.0))
      .as("rowid", "predicted")

    // Evaluation
    val eval = predict
      .join(testDf, predict("rowid") === testDf("rowid"))
      .where($"target" === $"predicted")

    val precision = (eval.count + 0.0) / predict.count

    TestUtils.expectResult(precision < 0.70,
      s"Low precision -> func:${func} value:${precision}")
  }

  // Only used in this local scope
  protected val LargeRegrTrainData = RegressionDatagen.exec(
    hiveContext, n_partitions = 2, min_examples = 100000, seed = 3, prob_one = 0.8f).cache

  protected val LargeRegrTestData = RegressionDatagen.exec(
    hiveContext, n_partitions = 2, min_examples = 100, seed = 3, prob_one = 0.5f).cache

  protected val LargeClassifierTrainData = RegressionDatagen.exec(
    hiveContext, n_partitions = 2, min_examples = 100000, seed = 5, prob_one = 0.8f, cl = true)
    .cache

  protected val LargeClassifierTestData = RegressionDatagen.exec(
    hiveContext, n_partitions = 2, min_examples = 100, seed = 5, prob_one = 0.5f, cl = true)
    .cache
}
