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

  test("knn.similarity") {
    val row1 = DummyInputData.select(cosine_sim(Seq(1, 2, 3, 4), Seq(3, 4, 5, 6))).collect
    assert(row1(0).getFloat(0) ~== 0.500f)

    val row2 = DummyInputData.select(jaccard(5, 6)).collect
    assert(row2(0).getFloat(0) ~== 0.96875f)

    val row3 = DummyInputData.select(angular_similarity(Seq(1, 2, 3), Seq(4, 5, 6))).collect
    assert(row3(0).getFloat(0) ~== 0.500f)

    val row4 = DummyInputData.select(euclid_similarity(Seq(5, 3, 1), Seq(2, 8, 3))).collect
    assert(row4(0).getFloat(0) ~== 0.33333334f)

    // TODO: $"c0" throws AnalysisException, why?
    // val row5 = DummyInputData.select(distance2similarity(DummyInputData("c0"))).collect
    // assert(row5(0).getFloat(0) ~== 0.1f)
  }

  test("knn.distance") {
    val row1 = DummyInputData.select(hamming_distance(1, 3)).collect
    assert(row1(0).getInt(0) == 1)

    val row2 = DummyInputData.select(popcnt(1)).collect
    assert(row2(0).getInt(0) == 1)

    val row3 = DummyInputData.select(kld(0.1, 0.5, 0.2, 0.5)).collect
    assert(row3(0).getDouble(0) ~== 0.01)

    val row4 = DummyInputData.select(
      euclid_distance(Seq("0.1", "0.5"), Seq("0.2", "0.5"))).collect
    assert(row4(0).getFloat(0) ~== 1.4142135f)

    val row5 = DummyInputData.select(
      cosine_distance(Seq("0.8", "0.3"), Seq("0.4", "0.6"))).collect
    assert(row5(0).getFloat(0) ~== 1.0f)

    val row6 = DummyInputData.select(
      angular_distance(Seq("0.1", "0.1"), Seq("0.3", "0.8"))).collect
    assert(row6(0).getFloat(0) ~== 0.50f)

    val row7 = DummyInputData.select(
      manhattan_distance(Seq("0.7", "0.8"), Seq("0.5", "0.6"))).collect
    assert(row7(0).getFloat(0) ~== 4.0f)

    val row8 = DummyInputData.select(
      minkowski_distance(Seq("0.1", "0.2"), Seq("0.2", "0.2"), 1.0)).collect
    assert(row8(0).getFloat(0) ~== 2.0f)
  }

  test("knn.lsh") {
    import hiveContext.implicits._
    assert(IntList2Data.minhash(1, $"target").count() > 0)

    assert(DummyInputData.select(bbit_minhash(Seq("1:0.1", "2:0.5"), false)).count
      == DummyInputData.count)
    assert(DummyInputData.select(minhashes(Seq("1:0.1", "2:0.5"), false)).count
      == DummyInputData.count)
  }

  test("ftvec - add_bias") {
    // TODO: This import does not work and why?
    // import hiveContext.implicits._
    assert(TinyTrainData.select(add_bias(TinyTrainData.col("features"))).collect.toSet
      === Set(
        Row(Seq("1:0.8", "2:0.2", "0:1.0")),
        Row(Seq("2:0.7", "0:1.0")),
        Row(Seq("1:0.9", "0:1.0"))))
  }

  test("ftvec - extract_feature") {
    val row = DummyInputData.select(extract_feature("1:0.8")).collect
    assert(row(0).getString(0) == "1")
  }

  test("ftvec - extract_weight") {
    val row = DummyInputData.select(extract_weight("3:0.1")).collect
    assert(row(0).getDouble(0) ~== 0.1)
  }

  test("ftvec - explode_array") {
    import hiveContext.implicits._
    assert(TinyTrainData.explode_array("features")
        .select($"feature").collect.toSet
      === Set(Row("1:0.8"), Row("2:0.2"), Row("2:0.7"), Row("1:0.9")))
  }

  test("ftvec - add_feature_index") {
    // import hiveContext.implicits._
    val doubleListData = {
      // TODO: Use `toDF`
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

  test("ftvec - sort_by_feature") {
    // import hiveContext.implicits._
    val intFloatMapData = {
      // TODO: Use `toDF`
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

  test("ftvec.hash") {
    assert(DummyInputData.select(mhash("test")).count == DummyInputData.count)
    assert(DummyInputData.select(sha1("test")).count == DummyInputData.count)
    // assert(DummyInputData.select(array_hash_values(Seq("aaa", "bbb"))).count
    //   == DummyInputData.count)
    // assert(DummyInputData.select(prefixed_hash_values(Seq("ccc", "ddd"), "prefix")).count
    //   == DummyInputData.count)
  }

  test("ftvec.scaling") {
    assert(TinyTrainData.select(rescale(2.0f, 1.0, 5.0)).collect.toSet
      === Set(Row(0.25f)))
    assert(TinyTrainData.select(zscore(1.0f, 0.5, 0.5)).collect.toSet
      === Set(Row(1.0f)))
    assert(TinyTrainData.select(normalize(TinyTrainData.col("features"))).collect.toSet
      === Set(
        Row(Seq("1:0.9701425", "2:0.24253562")),
        Row(Seq("2:1.0")),
        Row(Seq("1:1.0"))))
  }

  test("ftvec.conv - quantify") {
    import hiveContext.implicits._
    val testDf = Seq((1, "aaa", true), (2, "bbb", false), (3, "aaa", false)).toDF
    // This test is done in a single parition because `HivemallOps#quantify` assigns indentifiers
    // for non-numerical values in each partition.
    assert(testDf.coalesce(1).quantify(Seq[Column](true) ++ testDf.cols: _*).collect.toSet
      === Set(Row(1, 0, 0), Row(2, 1, 1), Row(3, 0, 1)))
  }

  test("ftvec.amplify") {
    import hiveContext.implicits._
    assert(TinyTrainData.amplify(3, $"label", $"features").count() == 9)
    assert(TinyTrainData.rand_amplify(3, "-buf 128", $"label", $"features").count() == 9)
    assert(TinyTrainData.part_amplify(3).count() == 9)
  }

  ignore("ftvec.conv") {
    import hiveContext.implicits._

    val df1 = Seq((0.0, "1:0.1" :: "3:0.3" :: Nil), (1,0, "2:0.2" :: Nil)).toDF("a", "b")
    assert(df1.select(to_dense_features(df1("b"), 3)).collect.toSet
      === Set(Row(Array(0.1f, 0.0f, 0.3f)), Array(0.0f, 0.2f, 0.0f)))

    val df2 = Seq((0.1, 0.2, 0.3), (0.2, 0.5, 0.4)).toDF("a", "b", "c")
    assert(df2.select(to_sparse_features(df2("a"), df2("b"), df2("c"))).collect.toSet
      === Set(Row(Seq("1:0.1", "2:0.2", "3:0.3")), Row(Seq("1:0.2", "2:0.5", "3:0.4"))))
  }

  test("ftvec.trans") {
    import hiveContext.implicits._

    val df1 = Seq((1, -3, 1), (2, -2, 1)).toDF("a", "b", "c")
    assert(df1.binarize_label($"a", $"b", $"c").collect.toSet === Set(Row(1, 1)))

    val df2 = Seq((0.1f, 0.2f), (0.5f, 0.3f)).toDF("a", "b")
    assert(df2.select(vectorize_features(Seq("a", "b"), df2("a"), df2("b"))).collect.toSet
      === Set(Row(Seq("a:0.1", "b:0.2")), Row(Seq("a:0.5", "b:0.3"))))

    val df3 = Seq(("c11", "c12"), ("c21", "c22")).toDF("a", "b")
    assert(df3.select(categorical_features(Seq("a", "b"), df3("a"), df3("b"))).collect.toSet
      === Set(Row(Seq("a#c11", "b#c12")), Row(Seq("a#c21", "b#c22"))))

    val df4 = Seq((0.1, 0.2, 0.3), (0.2, 0.5, 0.4)).toDF("a", "b", "c")
    assert(df4.select(indexed_features(df4("a"), df4("b"), df4("c"))).collect.toSet
      === Set(Row(Seq("1:0.1", "2:0.2", "3:0.3")), Row(Seq("1:0.2", "2:0.5", "3:0.4"))))

    val df5 = Seq(("xxx", "yyy", 0), ("zzz", "yyy", 1)).toDF("a", "b", "c")
    assert(df5.coalesce(1).quantified_features(true, df5("a"), df5("b"), df5("c")).collect.toSet
      === Set(Row(Seq(0.0, 0.0, 0.0)), Row(Seq(1.0, 0.0, 1.0))))

    val df6 = Seq((0.1, 0.2), (0.5, 0.3)).toDF("a", "b")
    assert(df6.select(quantitative_features(Seq("a", "b"), df6("a"), df6("b"))).collect.toSet
      === Set(Row(Seq("a:0.1", "b:0.2")), Row(Seq("a:0.5", "b:0.3"))))
  }

  test("misc - hivemall_version") {
    assert(DummyInputData.select(hivemall_version()).collect.toSet === Set(Row("0.4.2-rc.2")))
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

  test("misc - rowid") {
    val df = DummyInputData.select(rowid())
    assert(df.distinct.count == df.count)
  }

  test("misc - each_top_k") {
    import hiveContext.implicits._
    val testDf = Seq(
      ("a", "1", 0.5), ("b", "5", 0.1), ("a", "3", 0.8), ("c", "6", 0.3), ("b", "4", 0.3),
      ("a", "2", 0.6)
    ).toDF("group", "attr", "value")

    // Compute top-1 rows for each group
    assert(
      testDf.each_top_k(1, $"group", $"value", testDf("attr")).collect.toSet ===
      Set(
        Row(1, 0.8, "3"),
        Row(1, 0.3, "4"),
        Row(1, 0.3, "6")
      ))

    // Compute reverse top-1 rows for each group
    assert(
      testDf.each_top_k(-1, $"group", $"value", testDf("attr")).collect.toSet ===
      Set(
        Row(1, 0.5, "1"),
        Row(1, 0.1, "5"),
        Row(1, 0.3, "6")
      ))
  }

  /**
   * This test fails because;
   *
   * Cause: java.lang.OutOfMemoryError: Java heap space
   * at hivemall.smile.tools.RandomForestEnsembleUDAF$Result.<init>(RandomForestEnsembleUDAF.java:128)
   * at hivemall.smile.tools.RandomForestEnsembleUDAF$RandomForestPredictUDAFEvaluator.terminate(RandomForestEnsembleUDAF.java:91)
   * at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
   */
  ignore("misc - tree_predict") {
    import hiveContext.implicits._

    val model = Seq((0.0, 0.1 :: 0.1 :: Nil), (1.0, 0.2 :: 0.3 :: 0.2 :: Nil))
      .toDF("label", "features")
      .train_randomforest_regr($"features", $"label", "-trees 2")

    val testData = Seq((0.0, 0.1 :: 0.0 :: Nil), (1.0, 0.3 :: 0.5 :: 0.4 :: Nil))
      .toDF("label", "features")
      .select(rowid(), $"label", $"features")

    val predicted = model
      .join(testData).coalesce(1)
      .select(
        $"rowid",
        tree_predict(
          model("model_id"), model("model_type"), model("pred_model"), testData("features"), true)
            .as("predicted")
      )
      .groupby($"rowid")
      .rf_ensemble("predicted").as("rowid", "predicted")
      .select($"predicted.label")

    checkAnswer(predicted, Seq(Row(0), Row(1)))
  }

  test("misc - sigmoid") {
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
    val rows = DummyInputData.select(sigmoid($"c0")).collect
    assert(rows(0).getDouble(0) ~== 0.500)
    assert(rows(1).getDouble(0) ~== 0.731)
    assert(rows(2).getDouble(0) ~== 0.880)
    assert(rows(3).getDouble(0) ~== 0.952)
  }

  test("misc - lr_datagen") {
    assert(TinyTrainData.lr_datagen("-n_examples 100 -n_features 10 -seed 100").count >= 100)
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

    val df1 = Seq((1, 0.1f), (1, 0.2f), (2, 0.1f)).toDF.as("c0", "c1")
    val row1 = df1.groupby($"c0").voted_avg("c1").collect
    assert(row1(0).getDouble(1) ~== 0.15)
    assert(row1(1).getDouble(1) ~== 0.10)

    val df2 = Seq((1, 0.6f), (2, 0.2f), (1, 0.2f)).toDF.as("c0", "c1")
    val row2 = df2.groupby($"c0").agg("c1"->"voted_avg").collect
    assert(row2(0).getDouble(1) ~== 0.40)
    assert(row2(1).getDouble(1) ~== 0.20)

    val df3 = Seq((1, 0.2f), (1, 0.8f), (2, 0.3f)).toDF.as("c0", "c1")
    val row3 = df3.groupby($"c0").weight_voted_avg("c1").collect
    assert(row3(0).getDouble(1) ~== 0.50)
    assert(row3(1).getDouble(1) ~== 0.30)

    val df4 = Seq((1, 0.1f), (2, 0.9f), (1, 0.1f)).toDF.as("c0", "c1")
    val row4 = df4.groupby($"c0").agg("c1"->"weight_voted_avg").collect
    assert(row4(0).getDouble(1) ~== 0.10)
    assert(row4(1).getDouble(1) ~== 0.90)

    val df5 = Seq((1, 0.2f, 0.1f), (1, 0.4f, 0.2f), (2, 0.8f, 0.9f)).toDF.as("c0", "c1", "c2")
    val row5 = df5.groupby($"c0").argmin_kld("c1", "c2").collect
    assert(row5(0).getFloat(1) ~== 0.266666666)
    assert(row5(1).getFloat(1) ~== 0.80)

    val df6 = Seq((1, "id-0", 0.2f), (1, "id-1", 0.4f), (1, "id-2", 0.1f)).toDF.as("c0", "c1", "c2")
    val row6 = df6.groupby($"c0").max_label("c2", "c1").collect
    assert(row6(0).getString(1) == "id-1")

    val df7 = Seq((1, "id-0", 0.5f), (1, "id-1", 0.1f), (1, "id-2", 0.2f)).toDF.as("c0", "c1", "c2")
    val row7 = df7.groupby($"c0").maxrow("c2", "c1").as("c0", "c1").select($"c1.col1").collect
    assert(row7(0).getString(0) == "id-0")

    val df8 = Seq((1, 1), (1, 2), (2, 1), (1, 5)).toDF.as("c0", "c1")
    val row8 = df8.groupby($"c0").rf_ensemble("c1").as("c0", "c1").select("c1.probability").collect
    assert(row8(0).getDouble(0) ~== 0.3333333333)
    assert(row8(1).getDouble(0) ~== 1.0)

    val df9 = Seq((1, 3), (1, 8), (2, 9), (1, 1)).toDF.as("c0", "c1")
    val row9 = df9.groupby($"c0").agg("c1"->"rf_ensemble").as("c0", "c1")
      .select("c1.probability").collect
    assert(row9(0).getDouble(0) ~== 0.3333333333)
    assert(row9(1).getDouble(0) ~== 1.0)
  }

  test("user-defined aggregators for evaluation") {
    import hiveContext.implicits._

    val df1 = Seq((1, 1.0f, 0.5f), (1, 0.3f, 0.5f), (1, 0.1f, 0.2f)).toDF.as("c0", "c1", "c2")
    val row1 = df1.groupby($"c0").mae("c1", "c2").collect
    assert(row1(0).getDouble(1) ~== 0.26666666)

    val df2 = Seq((1, 0.3f, 0.8f), (1, 1.2f, 2.0f), (1, 0.2f, 0.3f)).toDF.as("c0", "c1", "c2")
    val row2 = df2.groupby($"c0").mse("c1", "c2").collect
    assert(row2(0).getDouble(1) ~== 0.29999999)

    val df3 = Seq((1, 0.3f, 0.8f), (1, 1.2f, 2.0f), (1, 0.2f, 0.3f)).toDF.as("c0", "c1", "c2")
    val row3 = df3.groupby($"c0").rmse("c1", "c2").collect
    assert(row3(0).getDouble(1) ~== 0.54772253)

    val df4 = Seq((1, Array(1, 2), Array(2, 3)), (1, Array(3, 8), Array(5, 4))).toDF
      .as("c0", "c1", "c2")
    val row4 = df4.groupby($"c0").f1score("c1", "c2").collect
    assert(row4(0).getDouble(1) ~== 0.25)
  }
}
