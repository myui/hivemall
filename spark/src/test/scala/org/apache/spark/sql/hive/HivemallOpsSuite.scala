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

package org.apache.spark.sql.hive

import scala.collection.mutable.Seq

import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.hive.HivemallOps._
import org.apache.spark.sql.hive.HivemallUtils._
import org.apache.spark.sql.types._
import org.apache.spark.test.HivemallQueryTest
import org.apache.spark.test.TestDoubleWrapper._
import org.apache.spark.test.TestUtils._

final class HivemallOpsSuite extends HivemallQueryTest {

  test("hivemall_version") {
    assert(DummyInputData.select(hivemall_version()).collect.toSet === Set(Row("0.4.1-alpha.6")))
    /**
     * TODO: Why a test below does fail?
     *
     * checkAnswer(
     *   DummyInputData.select(hivemall_version()).distinct,
     *   Row("0.3.1")
     * )
     *
     * The test throw an exception below:
     *
     * [info] - hivemall_version *** FAILED ***
     * [info]   org.apache.spark.sql.AnalysisException:
     *    Cannot resolve column name "HiveSimpleUDF#hivemall.HivemallVersionUDF()" among
     *    (HiveSimpleUDF#hivemall.Hivemall VersionUDF());
     * [info]   at org.apache.spark.sql.DataFrame$$anonfun$resolve$1.apply(DataFrame.scala:159)
     * [info]   at org.apache.spark.sql.DataFrame$$anonfun$resolve$1.apply(DataFrame.scala:159)
     * [info]   at scala.Option.getOrElse(Option.scala:120)
     * [info]   at org.apache.spark.sql.DataFrame.resolve(DataFrame.scala:158)
     * [info]   at org.apache.spark.sql.DataFrame$$anonfun$30.apply(DataFrame.scala:1227)
     * [info]   at org.apache.spark.sql.DataFrame$$anonfun$30.apply(DataFrame.scala:1227)
     * [info]   at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:244)
     * ...
     */
  }

  test("bbit_minhash") {
    // Assume no exception
    assert(DummyInputData.select(bbit_minhash(Seq("1:0.1", "2:0.5"), false)).count
      == DummyInputData.count)
  }

  test("minhashes") {
    // Assume no exception
    assert(DummyInputData.select(minhashes(Seq("1:0.1", "2:0.5"), false)).count
      == DummyInputData.count)
  }

  test("cosine_sim") {
    val row = DummyInputData.select(cosine_sim(Seq(1, 2, 3, 4), Seq(3, 4, 5, 6))).collect
    assert(row(0).getFloat(0) ~== 0.500f)
  }

  test("hamming_distance") {
    val row = DummyInputData.select(hamming_distance(1, 3)).collect
    assert(row(0).getInt(0) == 1)
  }

  test("jaccard") {
    val row = DummyInputData.select(jaccard(5, 6)).collect
    assert(row(0).getFloat(0) ~== 0.96875f)
  }

  test("popcnt") {
    val row = DummyInputData.select(popcnt(1)).collect
    assert(row(0).getInt(0) == 1)
  }

  test("kld") {
    val row = DummyInputData.select(kld(0.1, 0.5, 0.2, 0.5)).collect
    assert(row(0).getDouble(0) ~== 0.01)
  }

  test("add_bias") {
    // TODO: This import does not work and why?
    // import hiveContext.implicits._
    assert(TinyTrainData.select(add_bias(TinyTrainData.col("features"))).collect.toSet
      === Set(
        Row(Seq("1:0.8", "2:0.2", "0:1.0")),
        Row(Seq("2:0.7", "0:1.0")),
        Row(Seq("1:0.9", "0:1.0"))))
  }

  test("extract_feature") {
    val row = DummyInputData.select(extract_feature("1:0.8")).collect
    assert(row(0).getString(0) == "1")
  }

  test("extract_weight") {
    val row = DummyInputData.select(extract_weight("3:0.1")).collect
    assert(row(0).getDouble(0) ~== 0.1)
  }

  test("explode_array") {
    import hiveContext.implicits._
    assert(TinyTrainData.explode_array("features")
        .select($"feature").collect.toSet
      === Set(Row("1:0.8"), Row("2:0.2"), Row("2:0.7"), Row("1:0.9")))
  }

  test("add_feature_index") {
    // import hiveContext.implicits._
    val doubleListData = {
      val rowRdd = hiveContext.sparkContext.parallelize(
          Row(0.8 :: 0.5 :: Nil) ::
          Row(0.3 :: 0.1 :: Nil) ::
          Row(0.2 :: Nil) ::
          Nil
        )
      hiveContext.createDataFrame(
        rowRdd,
        StructType(
          StructField("data", ArrayType(DoubleType), true) ::
          Nil)
        )
    }

    assert(doubleListData.select(
        add_feature_index(doubleListData.col("data"))).collect.toSet
      === Set(
        Row(Seq("1:0.8", "2:0.5")),
        Row(Seq("1:0.3", "2:0.1")),
        Row(Seq("1:0.2"))))
  }

  test("each_top_k") {
    // import hiveContext.implicits._
    val groupedData = {
      val rowRdd = hiveContext.sparkContext.parallelize(
          Row("a", "1", 0.5) ::
          Row("a", "2", 0.6) ::
          Row("a", "3", 0.8) ::
          Row("b", "4", 0.3) ::
          Row("b", "5", 0.1) ::
          Row("c", "6", 0.3) ::
          Nil
        )
      hiveContext.createDataFrame(
        rowRdd,
        StructType(
          StructField("group", StringType, true) ::
          StructField("attr", StringType, true) ::
          StructField("value", DoubleType, true) ::
          Nil)
        )
    }

    // Compute top-1 rows for each group
    val top1 = groupedData.each_top_k(
      1, groupedData.col("group"), groupedData.col("value"), groupedData.col("attr"))

    assert(top1.select(top1.col("attr")).collect.toSet ===
      Set(Row("3"), Row("4"), Row("6")))
  }

  test("mhash") {
    // Assume no exception
    assert(DummyInputData.select(mhash("test")).count == DummyInputData.count)
  }

  test("sha1") {
    // Assume no exception
    assert(DummyInputData.select(sha1("test")).count == DummyInputData.count)
  }

  test("rowid") {
    val df = DummyInputData.select(rowid())
    assert(df.distinct.count == df.count)
  }

  // TODO: Support testing equality between two floating points
  test("rescale") {
   assert(TinyTrainData.select(rescale(2.0f, 1.0, 5.0)).collect.toSet
      === Set(Row(0.25f)))
  }

  test("zscore") {
   assert(TinyTrainData.select(zscore(1.0f, 0.5, 0.5)).collect.toSet
      === Set(Row(1.0f)))
  }

  test("normalize") {
    // import hiveContext.implicits._
    assert(TinyTrainData.select(normalize(TinyTrainData.col("features"))).collect.toSet
      === Set(
        Row(Seq("1:0.9701425", "2:0.24253562")),
        Row(Seq("2:1.0")),
        Row(Seq("1:1.0"))))
  }

  test("quantify") {
    import hiveContext.implicits._
    val testDf = Seq((1, "aaa", true), (2, "bbb", false), (3, "aaa", false)).toDF
    // This test is done in a single parition because `HivemallOps#quantify` assigns indentifiers
    // for non-numerical values in each partition.
    assert(testDf.coalesce(1).quantify(Seq[Column](true) ++ testDf.cols: _*).collect.toSet
      === Set(Row(1, 0, 0), Row(2, 1, 1), Row(3, 0, 1)))
  }

  test("sigmoid") {
    import hiveContext.implicits._
    /**
     * TODO: SigmodUDF only accepts floating-point types in spark-v1.5.0?
     * This test throws an exception below:
     *
     * [info]   org.apache.spark.sql.catalyst.analysis.UnresolvedException:
     *    Invalid call to dataType on unresolved object, tree: 'data
     * [info]   at org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute.dataType(unresolved.scala:59)
     * [info]   at org.apache.spark.sql.hive.HiveSimpleUDF$$anonfun$method$1.apply(hiveUDFs.scala:119)
     * ...
     */
    val rows = DummyInputData.select(sigmoid($"data")).collect
    assert(rows(0).getDouble(0) ~== 0.500)
    assert(rows(1).getDouble(0) ~== 0.731)
    assert(rows(2).getDouble(0) ~== 0.880)
    assert(rows(3).getDouble(0) ~== 0.952)
  }

  test("sort_by_feature") {
    // import hiveContext.implicits._
    val intFloatMapData = {
      val rowRdd = hiveContext.sparkContext.parallelize(
          Row(Map(1->0.3f, 2->0.1f, 3->0.5f)) ::
          Row(Map(2->0.4f, 1->0.2f)) ::
          Row(Map(2->0.4f, 3->0.2f, 1->0.1f, 4->0.6f)) ::
          Nil
        )
      hiveContext.createDataFrame(
        rowRdd,
        StructType(
          StructField("data", MapType(IntegerType, FloatType), true) ::
          Nil)
        )
    }

    val sortedKeys = intFloatMapData.select(sort_by_feature(intFloatMapData.col("data")))
      .collect.map {
        case Row(m: Map[Int, Float]) => m.keysIterator.toSeq
    }

    assert(sortedKeys.toSet === Set(Seq(1, 2, 3), Seq(1, 2), Seq(1, 2, 3, 4)))
  }

  test("invoke regression functions") {
    import hiveContext.implicits._
    Seq(
      "train_adadelta",
      "train_adagrad",
      "train_arow_regr",
      "train_arowe_regr",
      "train_arowe2_regr",
      "train_logregr",
      "train_pa1_regr",
      "train_pa1a_regr",
      "train_pa2_regr",
      "train_pa2a_regr"
    ).map { func =>
      invokeFunc(new HivemallOps(TinyTrainData), func, Seq($"features", $"label"))
        .foreach(_ => {}) // Just call it
    }
  }

  test("invoke classifier functions") {
    import hiveContext.implicits._
    Seq(
      "train_perceptron",
      "train_pa",
      "train_pa1",
      "train_pa2",
      "train_cw",
      "train_arow",
      "train_arowh",
      "train_scw",
      "train_scw2",
      "train_adagrad_rda"
    ).map { func =>
      invokeFunc(new HivemallOps(TinyTrainData), func, Seq($"features", $"label"))
        .foreach(_ => {}) // Just call it
    }
  }

  test("invoke multiclass classifier functions") {
    import hiveContext.implicits._
    Seq(
      "train_multiclass_perceptron",
      "train_multiclass_pa",
      "train_multiclass_pa1",
      "train_multiclass_pa2",
      "train_multiclass_cw",
      "train_multiclass_arow",
      "train_multiclass_scw",
      "train_multiclass_scw2"
    ).map { func =>
      // TODO: Why is a label type [Int|Text] only in multiclass classifiers?
      invokeFunc(new HivemallOps(TinyTrainData), func, Seq($"features", $"label".cast(IntegerType)))
        .foreach(_ => {}) // Just call it
    }
  }

  test("invoke random forest functions") {
    import hiveContext.implicits._
    val testDf = Seq(
      (Array(0.3, 0.1, 0.2), 1),
      (Array(0.3, 0.1, 0.2), 0),
      (Array(0.3, 0.1, 0.2), 0)).toDF.as("features", "label")
    Seq(
      "train_randomforest_regr",
      "train_randomforest_classifier"
    ).map { func =>
      invokeFunc(new HivemallOps(testDf.coalesce(1)), func, Seq($"features", $"label"))
        .foreach(_ => {}) // Just call it
    }
  }

  test("invoke misc udtf functions") {
    import hiveContext.implicits._
    Seq("minhash").map { func =>
      invokeFunc(new HivemallOps(TinyTrainData), func, Seq($"label", $"features"))
        .foreach(_ => {}) // Just call it
    }
  }

  ignore("check regression precision") {
    Seq(
      "train_adadelta",
      "train_adagrad",
      "train_arow_regr",
      "train_arowe_regr",
      "train_arowe2_regr",
      "train_logregr",
      "train_pa1_regr",
      "train_pa1a_regr",
      "train_pa2_regr",
      "train_pa2a_regr"
    ).map { func =>
      checkRegrPrecision(func)
    }
  }

  ignore("check classifier precision") {
    Seq(
      "train_perceptron",
      "train_pa",
      "train_pa1",
      "train_pa2",
      "train_cw",
      "train_arow",
      "train_arowh",
      "train_scw",
      "train_scw2",
      "train_adagrad_rda"
    ).map { func =>
      checkClassifierPrecision(func)
    }
  }

  test("user-defined aggregators for ensembles") {
    import hiveContext.implicits._
    Seq("voted_avg", "weight_voted_avg")
      .map { udaf =>
        TinyScoreData.groupby().agg("score"->udaf)
          .foreach(_ => {})
      }
    Seq("rf_ensemble")
      .map { udaf =>
         Seq((1, 1), (1, 0), (0, 0)).toDF.as("c0", "c1")
           .groupby($"c0").agg("c1"->udaf)
           .foreach(_ => {})
      }
  }

  ignore("user-defined aggregators for evaluation") {
    /**
     * TODO: These tests throw an exception below:
     *
     * [info] - user-defined aggregators for evaluation *** FAILED ***
     * [info]   java.lang.AssertionError: assertion failed: Invoking mae failed because: null
     * [info]   at scala.Predef$.assert(Predef.scala:179)
     * [info]   at org.apache.spark.test.TestUtils$.invokeFunc(TestUtils.scala:46)
     * [info]   at org.apache.spark.sql.hive.HivemallOpsSuite$$anonfun$30$$anonfun$apply$mcV$sp$8
     *    .apply(HivemallOpsSuite.scala:363)
     * [info]   at org.apache.spark.sql.hive.HivemallOpsSuite$$anonfun$30$$anonfun$apply$mcV$sp$8
     *    .apply(HivemallOpsSuite.scala:362)
     * ...
     */
    Seq("mae", "mse", "rmse")
      .map { udaf =>
        invokeFunc(Float2Data.groupby(), udaf, "predict", "target")
      }
    Seq("f1score")
      .map { udaf =>
      invokeFunc(IntList2Data.groupby(), udaf, "predict", "target")
    }
  }

  test("amplify functions") {
    import hiveContext.implicits._
    assert(TinyTrainData.amplify(3, $"label", $"features").count() == 9)
    assert(TinyTrainData.rand_amplify(3, 128, $"label", $"features").count() == 9)
    assert(TinyTrainData.part_amplify(3).count() == 9)
  }

  test("lr_datagen") {
    assert(TinyTrainData.lr_datagen("-n_examples 100 -n_features 10 -seed 100").count >= 100)
  }
}
