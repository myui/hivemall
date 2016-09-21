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

import java.util.UUID

import org.apache.spark.Logging
import org.apache.spark.ml.feature.HivemallFeature
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Generate, LogicalPlan}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveShim.HiveFunctionWrapper
import org.apache.spark.sql.types._

/**
 * Hivemall wrapper and some utility functions for DataFrame.
 *
 * @groupname regression
 * @groupname classifier
 * @groupname classifier.multiclass
 * @groupname ensemble
 * @groupname knn.similarity
 * @groupname knn.distance
 * @groupname knn.lsh
 * @groupname ftvec
 * @groupname ftvec.amplify
 * @groupname ftvec.hashing
 * @groupname ftvec.scaling
 * @groupname ftvec.conv
 * @groupname ftvec.trans
 * @groupname misc
 */
final class HivemallOps(df: DataFrame) extends Logging {

  /**
   * An implicit conversion to avoid doing annoying transformation.
   */
  @inline
  private[this] implicit def toDataFrame(logicalPlan: LogicalPlan) =
    DataFrame(df.sqlContext, logicalPlan)

  /**
   * @see hivemall.regression.AdaDeltaUDTF
   * @group regression
   */
  @scala.annotation.varargs
  def train_adadelta(exprs: Column*): DataFrame = {
    Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.regression.AdaDeltaUDTF"),
        setMixServs(exprs: _*).map(_.expr)),
      join = false, outer = false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.regression.AdaGradUDTF
   * @group regression
   */
  @scala.annotation.varargs
  def train_adagrad(exprs: Column*): DataFrame = {
    Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.regression.AdaGradUDTF"),
        setMixServs(exprs: _*).map(_.expr)),
      join = false, outer = false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.regression.AROWRegressionUDTF
   * @group regression
   */
  @scala.annotation.varargs
  def train_arow_regr(exprs: Column*): DataFrame = {
    Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.regression.AROWRegressionUDTF"),
        setMixServs(exprs: _*).map(_.expr)),
      join = false, outer = false, None,
      Seq("feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.regression.AROWRegressionUDTF$AROWe
   * @group regression
   */
  @scala.annotation.varargs
  def train_arowe_regr(exprs: Column*): DataFrame = {
    Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.regression.AROWRegressionUDTF$AROWe"),
        setMixServs(exprs: _*).map(_.expr)),
      join = false, outer = false, None,
      Seq("feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.regression.AROWRegressionUDTF$AROWe2
   * @group regression
   */
  @scala.annotation.varargs
  def train_arowe2_regr(exprs: Column*): DataFrame = {
    Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.regression.AROWRegressionUDTF$AROWe2"),
        setMixServs(exprs: _*).map(_.expr)),
      join = false, outer = false, None,
      Seq("feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.regression.LogressUDTF
   * @group regression
   */
  @scala.annotation.varargs
  def train_logregr(exprs: Column*): DataFrame = {
    Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.regression.LogressUDTF"),
        setMixServs(exprs: _*).map(_.expr)),
      join = false, outer = false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.regression.PassiveAggressiveRegressionUDTF
   * @group regression
   */
  @scala.annotation.varargs
  def train_pa1_regr(exprs: Column*): DataFrame = {
     Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.regression.PassiveAggressiveRegressionUDTF"),
        setMixServs(exprs: _*).map(_.expr)),
      join = false, outer = false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.regression.PassiveAggressiveRegressionUDTF.PA1a
   * @group regression
   */
  @scala.annotation.varargs
  def train_pa1a_regr(exprs: Column*): DataFrame = {
     Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.regression.PassiveAggressiveRegressionUDTF$PA1a"),
        setMixServs(exprs: _*).map(_.expr)),
      join = false, outer = false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.regression.PassiveAggressiveRegressionUDTF.PA2
   * @group regression
   */
  @scala.annotation.varargs
  def train_pa2_regr(exprs: Column*): DataFrame = {
     Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.regression.PassiveAggressiveRegressionUDTF$PA2"),
        setMixServs(exprs: _*).map(_.expr)),
      join = false, outer = false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.regression.PassiveAggressiveRegressionUDTF.PA2a
   * @group regression
   */
  @scala.annotation.varargs
  def train_pa2a_regr(exprs: Column*): DataFrame = {
     Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.regression.PassiveAggressiveRegressionUDTF$PA2a"),
        setMixServs(exprs: _*).map(_.expr)),
      join = false, outer = false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.smile.regression.RandomForestRegressionUDTF
   * @group regression
   */
  @scala.annotation.varargs
  def train_randomforest_regr(exprs: Column*): DataFrame = {
     Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.smile.regression.RandomForestRegressionUDTF"),
        setMixServs(exprs: _*).map(_.expr)),
      join = false, outer = false, None,
      Seq("model_id", "model_type", "pred_model", "var_importance", "oob_errors", "oob_tests")
        .map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.PerceptronUDTF
   * @group classifier
   */
  @scala.annotation.varargs
  def train_perceptron(exprs: Column*): DataFrame = {
     Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.classifier.PerceptronUDTF"),
        setMixServs(exprs: _*).map(_.expr)),
      join = false, outer = false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.PassiveAggressiveUDTF
   * @group classifier
   */
  @scala.annotation.varargs
  def train_pa(exprs: Column*): DataFrame = {
     Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.classifier.PassiveAggressiveUDTF"),
        setMixServs(exprs: _*).map(_.expr)),
      join = false, outer = false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.PassiveAggressiveUDTF$PA1
   * @group classifier
   */
  @scala.annotation.varargs
  def train_pa1(exprs: Column*): DataFrame = {
     Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.classifier.PassiveAggressiveUDTF$PA1"),
        setMixServs(exprs: _*).map(_.expr)),
      join = false, outer = false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.PassiveAggressiveUDTF$PA2
   * @group classifier
   */
  @scala.annotation.varargs
  def train_pa2(exprs: Column*): DataFrame = {
     Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.classifier.PassiveAggressiveUDTF$PA2"),
        setMixServs(exprs: _*).map(_.expr)),
      join = false, outer = false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.ConfidenceWeightedUDTF
   * @group classifier
   */
  @scala.annotation.varargs
  def train_cw(exprs: Column*): DataFrame = {
     Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.classifier.ConfidenceWeightedUDTF"),
        setMixServs(exprs: _*).map(_.expr)),
      join = false, outer = false, None,
      Seq("feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.AROWClassifierUDTF
   * @group classifier
   */
  @scala.annotation.varargs
  def train_arow(exprs: Column*): DataFrame = {
     Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.classifier.AROWClassifierUDTF"),
        setMixServs(exprs: _*).map(_.expr)),
      join = false, outer = false, None,
      Seq("feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.AROWClassifierUDTF$AROWh
   * @group classifier
   */
  @scala.annotation.varargs
  def train_arowh(exprs: Column*): DataFrame = {
     Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.classifier.AROWClassifierUDTF$AROWh"),
        setMixServs(exprs: _*).map(_.expr)),
      join = false, outer = false, None,
      Seq("feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.SoftConfideceWeightedUDTF$SCW1
   * @group classifier
   */
  @scala.annotation.varargs
  def train_scw(exprs: Column*): DataFrame = {
     Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.classifier.SoftConfideceWeightedUDTF$SCW1"),
        setMixServs(exprs: _*).map(_.expr)),
      join = false, outer = false, None,
      Seq("feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.SoftConfideceWeightedUDTF$SCW1
   * @group classifier
   */
  @scala.annotation.varargs
  def train_scw2(exprs: Column*): DataFrame = {
     Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.classifier.SoftConfideceWeightedUDTF$SCW2"),
        setMixServs(exprs: _*).map(_.expr)),
      join = false, outer = false, None,
      Seq("feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.classifier.AdaGradRDAUDTF
   * @group classifier
   */
  @scala.annotation.varargs
  def train_adagrad_rda(exprs: Column*): DataFrame = {
     Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.classifier.AdaGradRDAUDTF"),
        setMixServs(exprs: _*).map(_.expr)),
      join = false, outer = false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.smile.classification.RandomForestClassifierUDTF
   * @group classifier
   */
  @scala.annotation.varargs
  def train_randomforest_classifier(exprs: Column*): DataFrame = {
     Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.smile.classification.RandomForestClassifierUDTF"),
        setMixServs(exprs: _*).map(_.expr)),
      join = false, outer = false, None,
      Seq("model_id", "model_type", "pred_model", "var_importance", "oob_errors", "oob_tests")
        .map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.classifier.MulticlassPerceptronUDTF
   * @group classifier.multiclass
   */
  @scala.annotation.varargs
  def train_multiclass_perceptron(exprs: Column*): DataFrame = {
     Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.classifier.multiclass.MulticlassPerceptronUDTF"),
        setMixServs(exprs: _*).map(_.expr)),
      join = false, outer = false, None,
      Seq("label", "feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.classifier.PassiveAggressiveUDTF
   * @group classifier.multiclass
   */
  @scala.annotation.varargs
  def train_multiclass_pa(exprs: Column*): DataFrame = {
     Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.classifier.multiclass.MulticlassPassiveAggressiveUDTF"),
        setMixServs(exprs: _*).map(_.expr)),
      join = false, outer = false, None,
      Seq("label", "feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.classifier.PassiveAggressiveUDTF$PA1
   * @group classifier.multiclass
   */
  @scala.annotation.varargs
  def train_multiclass_pa1(exprs: Column*): DataFrame = {
     Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.classifier.multiclass.MulticlassPassiveAggressiveUDTF$PA1"),
        setMixServs(exprs: _*).map(_.expr)),
      join = false, outer = false, None,
      Seq("label", "feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.classifier.PassiveAggressiveUDTF$PA2
   * @group classifier.multiclass
   */
  @scala.annotation.varargs
  def train_multiclass_pa2(exprs: Column*): DataFrame = {
     Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.classifier.multiclass.MulticlassPassiveAggressiveUDTF$PA2"),
        setMixServs(exprs: _*).map(_.expr)),
      join = false, outer = false, None,
      Seq("label", "feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.classifier.MulticlassConfidenceWeightedUDTF
   * @group classifier.multiclass
   */
  @scala.annotation.varargs
  def train_multiclass_cw(exprs: Column*): DataFrame = {
     Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.classifier.multiclass.MulticlassConfidenceWeightedUDTF"),
        setMixServs(exprs: _*).map(_.expr)),
      join = false, outer = false, None,
      Seq("label", "feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.classifier.MulticlassAROWClassifierUDTF
   * @group classifier.multiclass
   */
  @scala.annotation.varargs
  def train_multiclass_arow(exprs: Column*): DataFrame = {
     Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.classifier.multiclass.MulticlassAROWClassifierUDTF"),
        setMixServs(exprs: _*).map(_.expr)),
      join = false, outer = false, None,
      Seq("label", "feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.classifier.MulticlassSoftConfidenceWeightedUDTF$SCW1
   * @group classifier.multiclass
   */
  @scala.annotation.varargs
  def train_multiclass_scw(exprs: Column*): DataFrame = {
     Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.classifier.multiclass.MulticlassSoftConfidenceWeightedUDTF$SCW1"),
        setMixServs(exprs: _*).map(_.expr)),
      join = false, outer = false, None,
      Seq("label", "feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.classifier.MulticlassSoftConfidenceWeightedUDTF$SCW2
   * @group classifier.multiclass
   */
  @scala.annotation.varargs
  def train_multiclass_scw2(exprs: Column*): DataFrame = {
     Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.classifier.multiclass.MulticlassSoftConfidenceWeightedUDTF$SCW2"),
        setMixServs(exprs: _*).map(_.expr)),
      join = false, outer = false, None,
      Seq("label", "feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * Groups the [[DataFrame]] using the specified columns, so we can run aggregation on them.
   * See [[GroupedDataEx]] for all the available aggregate functions.
   *
   * TODO: This class bypasses the original GroupData
   * so as to support user-defined aggregations.
   * Need a more smart injection into existing DataFrame APIs.
   *
   * A list of added Hivemall UDAF:
   *  - voted_avg
   *  - weight_voted_avg
   *  - argmin_kld
   *  - max_label
   *  - maxrow
   *  - f1score
   *  - mae
   *  - mse
   *  - rmse
   *
   * @groupname ensemble
   */
  @scala.annotation.varargs
  def groupby(cols: Column*): GroupedDataEx = {
    new GroupedDataEx(df, cols.map(_.expr), GroupedData.GroupByType)
  }

  @scala.annotation.varargs
  def groupby(col1: String, cols: String*): GroupedDataEx = {
    val colNames: Seq[String] = col1 +: cols
    new GroupedDataEx(df, colNames.map(colName => df(colName).expr), GroupedData.GroupByType)
  }

  /**
   * @see hivemall.knn.lsh.MinHashUDTF
   * @group knn.lsh
   */
  @scala.annotation.varargs
  def minhash(exprs: Column*): DataFrame = {
     Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.knn.lsh.MinHashUDTF"),
        exprs.map(_.expr)),
      join = false, outer = false, None,
      Seq("clusterid", "item").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.ftvec.amplify.AmplifierUDTF
   * @group ftvec.amplify
   */
  @scala.annotation.varargs
  def amplify(exprs: Column*): DataFrame = {
    val outputAttr = exprs.drop(1).map {
      case Column(expr: NamedExpression) => UnresolvedAttribute(expr.name)
      case Column(expr: Expression) => UnresolvedAttribute(expr.prettyString)
    }
    Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.ftvec.amplify.AmplifierUDTF"),
        exprs.map(_.expr)),
      join = false, outer = false, None,
      outputAttr,
      df.logicalPlan)
  }

  /**
   * @see hivemall.ftvec.amplify.RandomAmplifierUDTF
   * @group ftvec.amplify
   */
  @scala.annotation.varargs
  def rand_amplify(exprs: Column*): DataFrame = {
    val outputAttr = exprs.drop(2).map {
      case Column(expr: NamedExpression) => UnresolvedAttribute(expr.name)
      case Column(expr: Expression) => UnresolvedAttribute(expr.prettyString)
    }
    Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.ftvec.amplify.RandomAmplifierUDTF"),
        exprs.map(_.expr)),
      join = false, outer = false, None,
      outputAttr,
      df.logicalPlan)
  }

  /**
   * Amplifies and shuffle data inside partitions.
   * @group ftvec.amplify
   */
  def part_amplify(xtimes: Int): DataFrame = {
    val rdd = df.rdd.mapPartitions({ iter =>
      val elems = iter.flatMap{ row =>
        Seq.fill[Row](xtimes)(row)
      }
      // Need to check how this shuffling affects results
      scala.util.Random.shuffle(elems)
    }, true)
    df.sqlContext.createDataFrame(rdd, df.schema)
  }

  /**
   * Quantifies input columns.
   * @see hivemall.ftvec.conv.QuantifyColumnsUDTF
   * @group ftvec.conv
   */
  @scala.annotation.varargs
  def quantify(exprs: Column*): DataFrame = {
    Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.ftvec.conv.QuantifyColumnsUDTF"),
        exprs.map(_.expr)),
      join = false, outer = false, None,
      (0 until exprs.size - 1).map(i => s"c$i").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.ftvec.trans.BinarizeLabelUDTF
   * @group ftvec.trans
   */
  @scala.annotation.varargs
  def binarize_label(exprs: Column*): DataFrame = {
    Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.ftvec.trans.BinarizeLabelUDTF"),
        exprs.map(_.expr)),
      join = false, outer = false, None,
      (0 until exprs.size - 1).map(i => s"c$i").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.ftvec.trans.QuantifiedFeaturesUDTF
   * @group ftvec.trans
   */
  @scala.annotation.varargs
  def quantified_features(exprs: Column*): DataFrame = {
    Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.ftvec.trans.QuantifiedFeaturesUDTF"),
        exprs.map(_.expr)),
      join = false, outer = false, None,
      Seq("features").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * Splits Seq[String] into pieces.
   * @group ftvec
   */
  def explode_array(expr: Column): DataFrame = {
    df.explode(expr) { case Row(v: Seq[_]) =>
      // Type erasure removes the component type in Seq
      v.map(s => HivemallFeature(s.asInstanceOf[String]))
    }
  }

  def explode_array(expr: String): DataFrame =
    this.explode_array(df(expr))

  /**
   * Returns a top-`k` records for each `group`.
   * @group misc
   */
  def each_top_k(k: Column, group: Column, value: Column, args: Column*): DataFrame = {
    val clusterDf = df.repartition(group).sortWithinPartitions(group)
    Generate(HiveGenericUDTF(
      new HiveFunctionWrapper("hivemall.tools.EachTopKUDTF"),
      (Seq(k, group, value) ++ args).map(_.expr)),
    join = false, outer = false, None,
    (Seq("rank", "key") ++ args.map(_.named.name)).map(UnresolvedAttribute(_)),
    clusterDf.logicalPlan)
  }

  /**
   * Returns a new [[DataFrame]] with columns renamed.
   * This is a wrapper for DataFrame#toDF.
   * @group misc
   */
  @scala.annotation.varargs
  def as(colNames: String*): DataFrame = df.toDF(colNames: _*)

  /**
   * Returns all the columns as Seq[Column] in this [[DataFrame]].
   * @group misc
   */
  def cols: Seq[Column] = {
    df.schema.fields.map(col => df.col(col.name)).toSeq
  }

  /**
   * @see hivemall.dataset.LogisticRegressionDataGeneratorUDTF
   * @group misc
   */
  @scala.annotation.varargs
  def lr_datagen(exprs: Column*): DataFrame = {
    Generate(HiveGenericUDTF(
        new HiveFunctionWrapper("hivemall.dataset.LogisticRegressionDataGeneratorUDTFWrapper"),
        exprs.map(_.expr)),
      join = false, outer = false, None,
      Seq("label", "features").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * :: Experimental ::
   * If a parameter '-mix' does not exist in a 3rd argument,
   * set it from an environmental variable
   * 'HIVEMALL_MIX_SERVERS'.
   *
   * TODO: This could work if '--deploy-mode' has 'client';
   * otherwise, we need to set HIVEMALL_MIX_SERVERS
   * in all possible spark workers.
   */
  private[this] def setMixServs(exprs: Column*): Seq[Column] = {
    val mixes = System.getenv("HIVEMALL_MIX_SERVERS")
    if (mixes != null && !mixes.isEmpty()) {
      val groupId = df.sqlContext.sparkContext.applicationId + "-" + UUID.randomUUID
      logInfo(s"set '${mixes}' as default mix servers (session: ${groupId})")
      exprs.size match {
        case 2 => exprs :+ Column(Literal.create(s"-mix ${mixes} -mix_session ${groupId}", StringType))
        /** TODO: Add codes in the case where exprs.size == 3. */
        case _ => exprs
      }
    } else {
      exprs
    }
  }
}

object HivemallOps {

  /**
   * Implicitly inject the [[HivemallOps]] into [[DataFrame]].
   */
  implicit def dataFrameToHivemallOps(df: DataFrame): HivemallOps =
    new HivemallOps(df)

  /**
   * An implicit conversion to avoid doing annoying transformation.
   */
  @inline private implicit def toColumn(expr: Expression) = Column(expr)

  /**
   * @see hivemall.HivemallVersionUDF
   * @group misc
   */
  def hivemall_version(): Column = {
    HiveSimpleUDF(new HiveFunctionWrapper(
      "hivemall.HivemallVersionUDF"), Nil)
  }

  /**
   * @see hivemall.knn.similarity.CosineSimilarityUDF
   * @group knn.similarity
   */
  @scala.annotation.varargs
  def cosine_sim(exprs: Column*): Column = {
    HiveGenericUDF(new HiveFunctionWrapper(
      "hivemall.knn.similarity.CosineSimilarityUDF"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.knn.similarity.JaccardIndexUDF
   * @group knn.similarity
   */
  @scala.annotation.varargs
  def jaccard(exprs: Column*): Column = {
    HiveSimpleUDF(new HiveFunctionWrapper(
      "hivemall.knn.similarity.JaccardIndexUDF"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.knn.similarity.AngularSimilarityUDF
   * @group knn.similarity
   */
  @scala.annotation.varargs
  def angular_similarity(exprs: Column*): Column = {
    HiveGenericUDF(new HiveFunctionWrapper(
      "hivemall.knn.similarity.AngularSimilarityUDF"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.knn.similarity.EuclidSimilarity
   * @group knn.similarity
   */
  @scala.annotation.varargs
  def euclid_similarity(exprs: Column*): Column = {
    HiveGenericUDF(new HiveFunctionWrapper(
      "hivemall.knn.similarity.EuclidSimilarity"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.knn.similarity.Distance2SimilarityUDF
   * @group knn.similarity
   */
  @scala.annotation.varargs
  def distance2similarity(exprs: Column*): Column = {
    // TODO: Need a wrapper class because of using unsupported types
    HiveGenericUDF(new HiveFunctionWrapper(
      "hivemall.knn.similarity.Distance2SimilarityUDF"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.knn.distance.HammingDistanceUDF
   * @group knn.distance
   */
  @scala.annotation.varargs
  def hamming_distance(exprs: Column*): Column = {
    HiveSimpleUDF(new HiveFunctionWrapper(
      "hivemall.knn.distance.HammingDistanceUDF"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.knn.distance.PopcountUDF
   * @group knn.distance
   */
  @scala.annotation.varargs
  def popcnt(exprs: Column*): Column = {
    HiveSimpleUDF(new HiveFunctionWrapper(
      "hivemall.knn.distance.PopcountUDF"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.knn.distance.KLDivergenceUDF
   * @group knn.distance
   */
  @scala.annotation.varargs
  def kld(exprs: Column*): Column = {
    HiveSimpleUDF(new HiveFunctionWrapper(
      "hivemall.knn.distance.KLDivergenceUDF"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.knn.distance.EuclidDistanceUDF
   * @group knn.distance
   */
  @scala.annotation.varargs
  def euclid_distance(exprs: Column*): Column = {
    HiveGenericUDF(new HiveFunctionWrapper(
      "hivemall.knn.distance.EuclidDistanceUDF"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.knn.distance.CosineDistanceUDF
   * @group knn.distance
   */
  @scala.annotation.varargs
  def cosine_distance(exprs: Column*): Column = {
    HiveGenericUDF(new HiveFunctionWrapper(
      "hivemall.knn.distance.CosineDistanceUDF"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.knn.distance.AngularDistanceUDF
   * @group knn.distance
   */
  @scala.annotation.varargs
  def angular_distance(exprs: Column*): Column = {
    HiveGenericUDF(new HiveFunctionWrapper(
      "hivemall.knn.distance.AngularDistanceUDF"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.knn.distance.ManhattanDistanceUDF
   * @group knn.distance
   */
  @scala.annotation.varargs
  def manhattan_distance(exprs: Column*): Column = {
    HiveGenericUDF(new HiveFunctionWrapper(
      "hivemall.knn.distance.ManhattanDistanceUDF"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.knn.distance.MinkowskiDistanceUDF
   * @group knn.distance
   */
  @scala.annotation.varargs
  def minkowski_distance (exprs: Column*): Column = {
    HiveGenericUDF(new HiveFunctionWrapper(
      "hivemall.knn.distance.MinkowskiDistanceUDF"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.knn.lsh.bBitMinHashUDF
   * @group knn.lsh
   */
  @scala.annotation.varargs
  def bbit_minhash(exprs: Column*): Column = {
    HiveSimpleUDF(new HiveFunctionWrapper(
      "hivemall.knn.lsh.bBitMinHashUDF"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.knn.lsh.MinHashesUDF
   * @group knn.lsh
   */
  @scala.annotation.varargs
  def minhashes(exprs: Column*): Column = {
    HiveGenericUDF(new HiveFunctionWrapper(
      "hivemall.knn.lsh.MinHashesUDFWrapper"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.ftvec.AddBiasUDF
   * @group ftvec
   */
  @scala.annotation.varargs
  def add_bias(exprs: Column*): Column = {
    HiveGenericUDF(new HiveFunctionWrapper(
      "hivemall.ftvec.AddBiasUDFWrapper"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.ftvec.ExtractFeatureUdf
   * @group ftvec
   *
   * TODO: This throws java.lang.ClassCastException because
   * HiveInspectors.toInspector has a bug in spark.
   * Need to fix it later.
   */
  def extract_feature(expr: Column): Column = {
    val hiveUdf = HiveGenericUDF(
      new HiveFunctionWrapper("hivemall.ftvec.ExtractFeatureUDFWrapper"),
      expr.expr :: Nil)
    Column(hiveUdf).as("feature")
  }

  /**
   * @see hivemall.ftvec.ExtractWeightUdf
   * @group ftvec
   *
   * TODO: This throws java.lang.ClassCastException because
   * HiveInspectors.toInspector has a bug in spark.
   * Need to fix it later.
   */
  def extract_weight(expr: Column): Column = {
    val hiveUdf = HiveGenericUDF(
      new HiveFunctionWrapper("hivemall.ftvec.ExtractWeightUDFWrapper"),
      expr.expr :: Nil)
    Column(hiveUdf).as("value")
  }

  /**
   * @see hivemall.ftvec.AddFeatureIndexUDFWrapper
   * @group ftvec
   */
  def add_feature_index(expr: Column): Column = {
    HiveGenericUDF(new HiveFunctionWrapper(
      "hivemall.ftvec.AddFeatureIndexUDFWrapper"), expr.expr :: Nil)
  }

  /**
   * @see hivemall.ftvec.SortByFeatureUDF
   * @group ftvec
   */
  def sort_by_feature(expr: Column): Column = {
    HiveGenericUDF(new HiveFunctionWrapper(
      "hivemall.ftvec.SortByFeatureUDFWrapper"), expr.expr :: Nil)
  }

  /**
   * @see hivemall.ftvec.hashing.MurmurHash3UDF
   * @group ftvec.hashing
   */
  def mhash(expr: Column): Column = {
    HiveSimpleUDF(new HiveFunctionWrapper(
      "hivemall.ftvec.hashing.MurmurHash3UDF"), expr.expr :: Nil)
  }

  /**
   * @see hivemall.ftvec.hashing.Sha1UDF
   * @group ftvec.hashing
   */
  def sha1(expr: Column): Column = {
    HiveSimpleUDF(new HiveFunctionWrapper(
      "hivemall.ftvec.hashing.Sha1UDF"), expr.expr :: Nil)
  }

  /**
   * @see hivemall.ftvec.hashing.ArrayHashValuesUDF
   * @group ftvec.hashing
   */
  @scala.annotation.varargs
  def array_hash_values(exprs: Column*): Column = {
    // TODO: Need a wrapper class because of using unsupported types
    HiveSimpleUDF(new HiveFunctionWrapper(
      "hivemall.ftvec.hashing.ArrayHashValuesUDF"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.ftvec.hashing.ArrayPrefixedHashValuesUDF
   * @group ftvec.hashing
   */
  @scala.annotation.varargs
  def prefixed_hash_values(exprs: Column*): Column = {
    // TODO: Need a wrapper class because of using unsupported types
    HiveSimpleUDF(new HiveFunctionWrapper(
      "hivemall.ftvec.hashing.ArrayPrefixedHashValuesUDF"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.ftvec.scaling.RescaleUDF
   * @group ftvec.scaling
   */
  @scala.annotation.varargs
  def rescale(exprs: Column*): Column = {
    HiveSimpleUDF(new HiveFunctionWrapper(
      "hivemall.ftvec.scaling.RescaleUDF"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.ftvec.scaling.ZScoreUDF
   * @group ftvec.scaling
   */
  @scala.annotation.varargs
  def zscore(exprs: Column*): Column = {
    HiveSimpleUDF(new HiveFunctionWrapper(
      "hivemall.ftvec.scaling.ZScoreUDF"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.ftvec.scaling.L2NormalizationUDF
   * @group ftvec.scaling
   */
  def normalize(expr: Column): Column = {
    HiveGenericUDF(new HiveFunctionWrapper(
      "hivemall.ftvec.scaling.L2NormalizationUDFWrapper"), expr.expr :: Nil)
  }

  /**
   * @see hivemall.ftvec.conv.ToDenseFeaturesUDF
   * @group ftvec.conv
   */
  @scala.annotation.varargs
  def to_dense_features(exprs: Column*): Column = {
    // TODO: Need a wrapper class because of using unsupported types
    HiveGenericUDF(new HiveFunctionWrapper(
      "hivemall.ftvec.conv.ToDenseFeaturesUDF"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.ftvec.conv.ToSparseFeaturesUDF
   * @group ftvec.conv
   */
  @scala.annotation.varargs
  def to_sparse_features(exprs: Column*): Column = {
    // TODO: Need a wrapper class because of using unsupported types
    HiveGenericUDF(new HiveFunctionWrapper(
      "hivemall.ftvec.conv.ToSparseFeaturesUDF"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.ftvec.trans.VectorizeFeaturesUDF
   * @group ftvec.trans
   */
  @scala.annotation.varargs
  def vectorize_features(exprs: Column*): Column = {
    HiveGenericUDF(new HiveFunctionWrapper(
      "hivemall.ftvec.trans.VectorizeFeaturesUDF"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.ftvec.trans.CategoricalFeaturesUDF
   * @group ftvec.trans
   */
  @scala.annotation.varargs
  def categorical_features(exprs: Column*): Column = {
    HiveGenericUDF(new HiveFunctionWrapper(
      "hivemall.ftvec.trans.CategoricalFeaturesUDF"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.ftvec.trans.IndexedFeatures
   * @group ftvec.trans
   */
  @scala.annotation.varargs
  def indexed_features(exprs: Column*): Column = {
    HiveGenericUDF(new HiveFunctionWrapper(
      "hivemall.ftvec.trans.IndexedFeatures"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.ftvec.trans.QuantitativeFeaturesUDF
   * @group ftvec.trans
   */
  @scala.annotation.varargs
  def quantitative_features(exprs: Column*): Column = {
    HiveGenericUDF(new HiveFunctionWrapper(
      "hivemall.ftvec.trans.QuantitativeFeaturesUDF"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.smile.tools.TreePredictUDF
   * @group misc
   */
  @scala.annotation.varargs
  def tree_predict(exprs: Column*): Column = {
    HiveGenericUDF(new HiveFunctionWrapper(
      "hivemall.smile.tools.TreePredictUDF"), exprs.map(_.expr))
  }

  /**
   * @see hivemall.tools.math.SigmoidUDF
   * @group misc
   */
  @scala.annotation.varargs
  def sigmoid(exprs: Column*): Column = {
    /**
     * TODO: SigmodUDF only accepts floating-point types in spark-v1.5.0?
     */
    val value = exprs.head
    val one: () => Literal = () => Literal.create(1.0, DoubleType)
    Column(one()) / (Column(one()) + exp(-value))
  }

  /**
   * @see hivemall.tools.mapred.RowIdUDF
   * @group misc
   */
  def rowid(): Column = {
    val hiveUdf = HiveGenericUDF(new HiveFunctionWrapper(
      "hivemall.tools.mapred.RowIdUDFWrapper"), Nil)
    hiveUdf.as("rowid")
  }
}
