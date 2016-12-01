/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.spark.sql.hive

import org.apache.spark.sql.{AnalysisException, Column, Row}
import org.apache.spark.sql.functions
import org.apache.spark.sql.hive.HivemallGroupedDataset._
import org.apache.spark.sql.hive.HivemallOps._
import org.apache.spark.sql.hive.HivemallUtils._
import org.apache.spark.sql.types._
import org.apache.spark.test.{HivemallFeatureQueryTest, TestUtils, VectorQueryTest}
import org.apache.spark.test.TestDoubleWrapper._

final class HivemallOpsWithFeatureSuite extends HivemallFeatureQueryTest {

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

  // test("ftvec - explode_array") {
  //   import hiveContext.implicits._
  //   assert(TinyTrainData.explode_array("features")
  //       .select($"feature").collect.toSet
  //     === Set(Row("1:0.8"), Row("2:0.2"), Row("2:0.7"), Row("1:0.9")))
  // }

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
          Row(Map(1 -> 0.3f, 2 -> 0.1f, 3 -> 0.5f)) ::
          Row(Map(2 -> 0.4f, 1 -> 0.2f)) ::
          Row(Map(2 -> 0.4f, 3 -> 0.2f, 1 -> 0.1f, 4 -> 0.6f)) ::
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

  test("ftvec.selection - chi2") {
    import hiveContext.implicits._

    // see also hivemall.ftvec.selection.ChiSquareUDFTest
    val df = Seq(
      Seq(
        Seq(250.29999999999998, 170.90000000000003, 73.2, 12.199999999999996),
        Seq(296.8, 138.50000000000003, 212.99999999999997, 66.3),
        Seq(329.3999999999999, 148.7, 277.59999999999997, 101.29999999999998)
      ) -> Seq(
        Seq(292.1666753739119, 152.70000455081467, 187.93333893418327, 59.93333511948589),
        Seq(292.1666753739119, 152.70000455081467, 187.93333893418327, 59.93333511948589),
        Seq(292.1666753739119, 152.70000455081467, 187.93333893418327, 59.93333511948589)))
      .toDF("arg0", "arg1")

    val result = df.select(chi2(df("arg0"), df("arg1"))).collect
    assert(result.length == 1)
    val chi2Val = result.head.getAs[Row](0).getAs[Seq[Double]](0)
    val pVal = result.head.getAs[Row](0).getAs[Seq[Double]](1)

    (chi2Val, Seq(10.81782088, 3.59449902, 116.16984746, 67.24482759))
      .zipped
      .foreach((actual, expected) => assert(actual ~== expected))

    (pVal, Seq(4.47651499e-03, 1.65754167e-01, 5.94344354e-26, 2.50017968e-15))
      .zipped
      .foreach((actual, expected) => assert(actual ~== expected))
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
    // assert(TinyTrainData.rand_amplify(3, "-buf 128", $"label", $"features").count() == 9)
    assert(TinyTrainData.part_amplify(3).count() == 9)
  }

  ignore("ftvec.conv") {
    import hiveContext.implicits._

    val df1 = Seq((0.0, "1:0.1" :: "3:0.3" :: Nil), (1, 0, "2:0.2" :: Nil)).toDF("a", "b")
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
     * - hivemall_version *** FAILED ***
     *  org.apache.spark.sql.AnalysisException:
     *    Cannot resolve column name "HiveSimpleUDF#hivemall.HivemallVersionUDF()" among
     *    (HiveSimpleUDF#hivemall.Hivemall VersionUDF());
     *   at org.apache.spark.sql.DataFrame$$anonfun$resolve$1.apply(DataFrame.scala:159)
     *   at org.apache.spark.sql.DataFrame$$anonfun$resolve$1.apply(DataFrame.scala:159)
     *   at scala.Option.getOrElse(Option.scala:120)
     *   at org.apache.spark.sql.DataFrame.resolve(DataFrame.scala:158)
     *   at org.apache.spark.sql.DataFrame$$anonfun$30.apply(DataFrame.scala:1227)
     *   at org.apache.spark.sql.DataFrame$$anonfun$30.apply(DataFrame.scala:1227)
     *   at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:244)
     *   ...
     */
  }

  test("misc - rowid") {
    val df = DummyInputData.select(rowid())
    assert(df.distinct.count == df.count)
  }

  test("misc - each_top_k") {
    import hiveContext.implicits._
    val testDf = Seq(
      ("a", "1", 0.5, Array(0, 1, 2)),
      ("b", "5", 0.1, Array(3)),
      ("a", "3", 0.8, Array(2, 5)),
      ("c", "6", 0.3, Array(1, 3)),
      ("b", "4", 0.3, Array(2)),
      ("a", "2", 0.6, Array(1))
    ).toDF("key", "value", "score", "data")

    // Compute top-1 rows for each group
    checkAnswer(
      testDf.each_top_k(1, "key", "score", "key", "value"),
      Row(1, "a", "3") ::
      Row(1, "b", "4") ::
      Row(1, "c", "6") ::
      Nil
    )
    checkAnswer(
      testDf.each_top_k(1, $"key", $"score", $"key", $"value"),
      Row(1, "a", "3") ::
      Row(1, "b", "4") ::
      Row(1, "c", "6") ::
      Nil
    )

    // Compute reverse top-1 rows for each group
    checkAnswer(
      testDf.each_top_k(-1, "key", "score", "key", "value"),
      Row(1, "a", "1") ::
      Row(1, "b", "5") ::
      Row(1, "c", "6") ::
      Nil
    )
    checkAnswer(
      testDf.each_top_k(-1, $"key", $"score", $"key", $"value"),
      Row(1, "a", "1") ::
      Row(1, "b", "5") ::
      Row(1, "c", "6") ::
      Nil
    )

    // Check if some exceptions thrown in case of some conditions
    assert(intercept[AnalysisException] { testDf.each_top_k(0.1, $"key", $"score") }
      .getMessage contains "`k` must be integer, however")
    assert(intercept[AnalysisException] { testDf.each_top_k(1, "key", "data") }
      .getMessage contains "must have a comparable type")
  }

  /**
   * This test fails because;
   *
   * Cause: java.lang.OutOfMemoryError: Java heap space
   *  at hivemall.smile.tools.RandomForestEnsembleUDAF$Result.<init>
   *    (RandomForestEnsembleUDAF.java:128)
   *  at hivemall.smile.tools.RandomForestEnsembleUDAF$RandomForestPredictUDAFEvaluator
   *    .terminate(RandomForestEnsembleUDAF.java:91)
   *  at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
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
      .groupBy($"rowid")
      .rf_ensemble("predicted").as("rowid", "predicted")
      .select($"predicted.label")

    checkAnswer(predicted, Seq(Row(0), Row(1)))
  }

  test("tools.array - select_k_best") {
    import hiveContext.implicits._

    val data = Seq(Seq(0, 1, 3), Seq(2, 4, 1), Seq(5, 4, 9))
    val df = data.map(d => (d, Seq(3, 1, 2), 2)).toDF("features", "importance_list", "k")

    checkAnswer(df.select(select_k_best(df("features"), df("importance_list"), df("k"))),
      data.map(s => Row(Seq(s(0).toDouble, s(2).toDouble))))
  }

  test("misc - sigmoid") {
    import hiveContext.implicits._
    /**
     * TODO: SigmodUDF only accepts floating-point types in spark-v1.5.0?
     * This test throws an exception below:
     *
     * org.apache.spark.sql.catalyst.analysis.UnresolvedException:
     *    Invalid call to dataType on unresolved object, tree: 'data
     *  at org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute.dataType
     *    (unresolved.scala:59)
     *  at org.apache.spark.sql.hive.HiveSimpleUDF$$anonfun$method$1.apply(hiveUDFs.scala:119)
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
      TestUtils.invokeFunc(new HivemallOps(TinyTrainData), func, Seq($"features", $"label"))
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
      TestUtils.invokeFunc(new HivemallOps(TinyTrainData), func, Seq($"features", $"label"))
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
      TestUtils.invokeFunc(
          new HivemallOps(TinyTrainData), func, Seq($"features", $"label".cast(IntegerType)))
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
      TestUtils.invokeFunc(new HivemallOps(testDf.coalesce(1)), func, Seq($"features", $"label"))
        .foreach(_ => {}) // Just call it
    }
  }

  protected def checkRegrPrecision(func: String): Unit = {
    import hiveContext.implicits._

    // Build a model
    val model = {
      val res = TestUtils.invokeFunc(new HivemallOps(LargeRegrTrainData),
        func, Seq(add_bias($"features"), $"label"))
      if (!res.columns.contains("conv")) {
        res.groupBy("feature").agg("weight" -> "avg")
      } else {
        res.groupBy("feature").argmin_kld("weight", "conv")
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
      .groupBy("rowid").sum("value")
      .as("rowid", "predicted")

    // Evaluation
    val eval = predict
      .join(testDf, predict("rowid") === testDf("rowid"))
      .groupBy()
      .agg(Map("target" -> "avg", "predicted" -> "avg"))
      .as("target", "predicted")

    val diff = eval.map {
      case Row(target: Double, predicted: Double) =>
        Math.abs(target - predicted)
    }.first

    TestUtils.expectResult(diff > 0.10, s"Low precision -> func:${func} diff:${diff}")
  }

  protected def checkClassifierPrecision(func: String): Unit = {
    import hiveContext.implicits._

    // Build a model
    val model = {
      val res = TestUtils.invokeFunc(new HivemallOps(LargeClassifierTrainData),
        func, Seq(add_bias($"features"), $"label"))
      if (!res.columns.contains("conv")) {
        res.groupBy("feature").agg("weight" -> "avg")
      } else {
        res.groupBy("feature").argmin_kld("weight", "conv")
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
      .groupBy("rowid").sum("value")
      /**
       * TODO: This sentence throws an exception below:
       *
       * WARN Column: Constructing trivially true equals predicate, 'rowid#1323 = rowid#1323'.
       * Perhaps you need to use aliases.
       */
      .select($"rowid", functions.when(sigmoid($"sum(value)") > 0.50, 1.0).otherwise(0.0))
      .as("rowid", "predicted")

    // Evaluation
    val eval = predict
      .join(testDf, predict("rowid") === testDf("rowid"))
      .where($"target" === $"predicted")

    val precision = (eval.count + 0.0) / predict.count

    TestUtils.expectResult(precision < 0.70, s"Low precision -> func:${func} value:${precision}")
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
    val row1 = df1.groupBy($"c0").voted_avg("c1").collect
    assert(row1(0).getDouble(1) ~== 0.15)
    assert(row1(1).getDouble(1) ~== 0.10)

    val df3 = Seq((1, 0.2f), (1, 0.8f), (2, 0.3f)).toDF.as("c0", "c1")
    val row3 = df3.groupBy($"c0").weight_voted_avg("c1").collect
    assert(row3(0).getDouble(1) ~== 0.50)
    assert(row3(1).getDouble(1) ~== 0.30)

    val df5 = Seq((1, 0.2f, 0.1f), (1, 0.4f, 0.2f), (2, 0.8f, 0.9f)).toDF.as("c0", "c1", "c2")
    val row5 = df5.groupBy($"c0").argmin_kld("c1", "c2").collect
    assert(row5(0).getFloat(1) ~== 0.266666666)
    assert(row5(1).getFloat(1) ~== 0.80)

    val df6 = Seq((1, "id-0", 0.2f), (1, "id-1", 0.4f), (1, "id-2", 0.1f)).toDF.as("c0", "c1", "c2")
    val row6 = df6.groupBy($"c0").max_label("c2", "c1").collect
    assert(row6(0).getString(1) == "id-1")

    val df7 = Seq((1, "id-0", 0.5f), (1, "id-1", 0.1f), (1, "id-2", 0.2f)).toDF.as("c0", "c1", "c2")
    val row7 = df7.groupBy($"c0").maxrow("c2", "c1").as("c0", "c1").select($"c1.col1").collect
    assert(row7(0).getString(0) == "id-0")

    val df8 = Seq((1, 1), (1, 2), (2, 1), (1, 5)).toDF.as("c0", "c1")
    val row8 = df8.groupBy($"c0").rf_ensemble("c1").as("c0", "c1").select("c1.probability").collect
    assert(row8(0).getDouble(0) ~== 0.3333333333)
    assert(row8(1).getDouble(0) ~== 1.0)
  }

  test("user-defined aggregators for evaluation") {
    import hiveContext.implicits._

    val df1 = Seq((1, 1.0f, 0.5f), (1, 0.3f, 0.5f), (1, 0.1f, 0.2f)).toDF.as("c0", "c1", "c2")
    val row1 = df1.groupBy($"c0").mae("c1", "c2").collect
    assert(row1(0).getDouble(1) ~== 0.26666666)

    val df2 = Seq((1, 0.3f, 0.8f), (1, 1.2f, 2.0f), (1, 0.2f, 0.3f)).toDF.as("c0", "c1", "c2")
    val row2 = df2.groupBy($"c0").mse("c1", "c2").collect
    assert(row2(0).getDouble(1) ~== 0.29999999)

    val df3 = Seq((1, 0.3f, 0.8f), (1, 1.2f, 2.0f), (1, 0.2f, 0.3f)).toDF.as("c0", "c1", "c2")
    val row3 = df3.groupBy($"c0").rmse("c1", "c2").collect
    assert(row3(0).getDouble(1) ~== 0.54772253)

    val df4 = Seq((1, Array(1, 2), Array(2, 3)), (1, Array(3, 8), Array(5, 4))).toDF
      .as("c0", "c1", "c2")
    val row4 = df4.groupBy($"c0").f1score("c1", "c2").collect
    assert(row4(0).getDouble(1) ~== 0.25)
  }

  test("user-defined aggregators for ftvec.trans") {
    import hiveContext.implicits._

    val df0 = Seq((1, "cat", "mammal", 9), (1, "dog", "mammal", 10), (1, "human", "mammal", 10),
      (1, "seahawk", "bird", 101), (1, "wasp", "insect", 3), (1, "wasp", "insect", 9),
      (1, "cat", "mammal", 101), (1, "dog", "mammal", 1), (1, "human", "mammal", 9))
      .toDF("col0", "cat1", "cat2", "cat3")
    val row00 = df0.groupBy($"col0").onehot_encoding("cat1")
    val row01 = df0.groupBy($"col0").onehot_encoding("cat1", "cat2", "cat3")

    val result000 = row00.collect()(0).getAs[Row](1).getAs[Map[String, Int]](0)
    val result01 = row01.collect()(0).getAs[Row](1)
    val result010 = result01.getAs[Map[String, Int]](0)
    val result011 = result01.getAs[Map[String, Int]](1)
    val result012 = result01.getAs[Map[String, Int]](2)

    assert(result000.keySet === Set("seahawk", "cat", "human", "wasp", "dog"))
    assert(result000.values.toSet === Set(1, 2, 3, 4, 5))
    assert(result010.keySet === Set("seahawk", "cat", "human", "wasp", "dog"))
    assert(result010.values.toSet === Set(1, 2, 3, 4, 5))
    assert(result011.keySet === Set("bird", "insect", "mammal"))
    assert(result011.values.toSet === Set(6, 7, 8))
    assert(result012.keySet === Set(1, 3, 9, 10, 101))
    assert(result012.values.toSet === Set(9, 10, 11, 12, 13))
  }

  test("user-defined aggregators for ftvec.selection") {
    import hiveContext.implicits._

    // see also hivemall.ftvec.selection.SignalNoiseRatioUDAFTest
    // binary class
    // +-----------------+-------+
    // |     features    | class |
    // +-----------------+-------+
    // | 5.1,3.5,1.4,0.2 |     0 |
    // | 4.9,3.0,1.4,0.2 |     0 |
    // | 4.7,3.2,1.3,0.2 |     0 |
    // | 7.0,3.2,4.7,1.4 |     1 |
    // | 6.4,3.2,4.5,1.5 |     1 |
    // | 6.9,3.1,4.9,1.5 |     1 |
    // +-----------------+-------+
    val df0 = Seq(
      (1, Seq(5.1, 3.5, 1.4, 0.2), Seq(1, 0)), (1, Seq(4.9, 3.0, 1.4, 0.2), Seq(1, 0)),
      (1, Seq(4.7, 3.2, 1.3, 0.2), Seq(1, 0)), (1, Seq(7.0, 3.2, 4.7, 1.4), Seq(0, 1)),
      (1, Seq(6.4, 3.2, 4.5, 1.5), Seq(0, 1)), (1, Seq(6.9, 3.1, 4.9, 1.5), Seq(0, 1)))
      .toDF("c0", "arg0", "arg1")
    val row0 = df0.groupBy($"c0").snr("arg0", "arg1").collect
    (row0(0).getAs[Seq[Double]](1), Seq(4.38425236, 0.26390002, 15.83984511, 26.87005769))
      .zipped
      .foreach((actual, expected) => assert(actual ~== expected))

    // multiple class
    // +-----------------+-------+
    // |     features    | class |
    // +-----------------+-------+
    // | 5.1,3.5,1.4,0.2 |     0 |
    // | 4.9,3.0,1.4,0.2 |     0 |
    // | 7.0,3.2,4.7,1.4 |     1 |
    // | 6.4,3.2,4.5,1.5 |     1 |
    // | 6.3,3.3,6.0,2.5 |     2 |
    // | 5.8,2.7,5.1,1.9 |     2 |
    // +-----------------+-------+
    val df1 = Seq(
      (1, Seq(5.1, 3.5, 1.4, 0.2), Seq(1, 0, 0)), (1, Seq(4.9, 3.0, 1.4, 0.2), Seq(1, 0, 0)),
      (1, Seq(7.0, 3.2, 4.7, 1.4), Seq(0, 1, 0)), (1, Seq(6.4, 3.2, 4.5, 1.5), Seq(0, 1, 0)),
      (1, Seq(6.3, 3.3, 6.0, 2.5), Seq(0, 0, 1)), (1, Seq(5.8, 2.7, 5.1, 1.9), Seq(0, 0, 1)))
      .toDF("c0", "arg0", "arg1")
    val row1 = df1.groupBy($"c0").snr("arg0", "arg1").collect
    (row1(0).getAs[Seq[Double]](1), Seq(8.43181818, 1.32121212, 42.94949495, 33.80952381))
      .zipped
      .foreach((actual, expected) => assert(actual ~== expected))
  }

  test("user-defined aggregators for tools.matrix") {
    import hiveContext.implicits._

    // | 1  2  3 |T    | 5  6  7 |
    // | 3  4  5 |  *  | 7  8  9 |
    val df0 = Seq((1, Seq(1, 2, 3), Seq(5, 6, 7)), (1, Seq(3, 4, 5), Seq(7, 8, 9)))
      .toDF("c0", "arg0", "arg1")

    checkAnswer(df0.groupBy($"c0").transpose_and_dot("arg0", "arg1"),
      Seq(Row(1, Seq(Seq(26.0, 30.0, 34.0), Seq(38.0, 44.0, 50.0), Seq(50.0, 58.0, 66.0)))))
  }
}

final class HivemallOpsWithVectorSuite extends VectorQueryTest {
  import hiveContext.implicits._

  test("to_hivemall_features") {
    checkAnswer(
      mllibTrainDf.select(to_hivemall_features($"features")),
      Seq(
        Row(Seq("0:1.0", "2:2.0", "4:3.0")),
        Row(Seq("0:1.0", "3:1.5", "4:2.1", "6:1.2")),
        Row(Seq("0:1.1", "3:1.0", "4:2.3", "6:1.0")),
        Row(Seq("1:4.0", "3:5.0", "5:6.0"))
      )
    )
  }

  ignore("append_bias") {
    /**
     * TODO: This test throws an exception:
     * Failed to analyze query: org.apache.spark.sql.AnalysisException: cannot resolve
     *   'UDF(UDF(features))' due to data type mismatch: argument 1 requires vector type,
     *    however, 'UDF(features)' is of vector type.; line 2 pos 8
     */
    checkAnswer(
      mllibTrainDf.select(to_hivemall_features(append_bias($"features"))),
      Seq(
        Row(Seq("0:1.0", "0:1.0", "2:2.0", "4:3.0")),
        Row(Seq("0:1.0", "0:1.0", "3:1.5", "4:2.1", "6:1.2")),
        Row(Seq("0:1.0", "0:1.1", "3:1.0", "4:2.3", "6:1.0")),
        Row(Seq("0:1.0", "1:4.0", "3:5.0", "5:6.0"))
      )
    )
  }

  test("explode_vector") {
    checkAnswer(
      mllibTrainDf.explode_vector($"features").select($"feature", $"weight"),
      Seq(
        Row("0", 1.0), Row("0", 1.0), Row("0", 1.1),
        Row("1", 4.0),
        Row("2", 2.0),
        Row("3", 1.0), Row("3", 1.5), Row("3", 5.0),
        Row("4", 2.1), Row("4", 2.3), Row("4", 3.0),
        Row("5", 6.0),
        Row("6", 1.0), Row("6", 1.2)
      )
    )
  }

  test("train_logregr") {
    checkAnswer(
      mllibTrainDf.train_logregr($"features", $"label")
        .groupBy("feature").agg("weight" -> "avg")
        .select($"feature"),
      Seq(0, 1, 2, 3, 4, 5, 6).map(v => Row(s"$v"))
    )
  }
}
