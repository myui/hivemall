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

import scala.collection.JavaConverters._

import org.apache.spark.annotation.Experimental
import org.apache.spark.internal.Logging
import org.apache.spark.ml.feature.HivemallFeature
import org.apache.spark.ml.linalg.{DenseVector => SDV, SparseVector => SSV, VectorUDT}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{EachTopK, Expression, Literal, NamedExpression, UserDefinedGenerator}
import org.apache.spark.sql.catalyst.plans.logical.{Generate, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveShim.HiveFunctionWrapper
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Hivemall wrapper and some utility functions for DataFrame.
 *
 * @groupname regression
 * @groupname classifier
 * @groupname classifier.multiclass
 * @groupname xgboost
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
  import HivemallOps._
  import HivemallUtils._

  /**
   * @see hivemall.regression.AdaDeltaUDTF
   * @group regression
   */
  @scala.annotation.varargs
  def train_adadelta(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "train_adadelta",
        new HiveFunctionWrapper("hivemall.regression.AdaDeltaUDTF"),
        setMixServs(toHivemallFeatureDf(exprs: _*)).map(_.expr)
      ),
      join = false, outer = false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.regression.AdaGradUDTF
   * @group regression
   */
  @scala.annotation.varargs
  def train_adagrad(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "train_adagrad",
        new HiveFunctionWrapper("hivemall.regression.AdaGradUDTF"),
        setMixServs(toHivemallFeatureDf(exprs: _*)).map(_.expr)
      ),
      join = false, outer = false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.regression.AROWRegressionUDTF
   * @group regression
   */
  @scala.annotation.varargs
  def train_arow_regr(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "train_arow_regr",
        new HiveFunctionWrapper("hivemall.regression.AROWRegressionUDTF"),
        setMixServs(toHivemallFeatureDf(exprs: _*)).map(_.expr)
      ),
      join = false, outer = false, None,
      Seq("feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.regression.AROWRegressionUDTF$AROWe
   * @group regression
   */
  @scala.annotation.varargs
  def train_arowe_regr(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "train_arowe_regr",
        new HiveFunctionWrapper("hivemall.regression.AROWRegressionUDTF$AROWe"),
        setMixServs(toHivemallFeatureDf(exprs: _*)).map(_.expr)
      ),
      join = false, outer = false, None,
      Seq("feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.regression.AROWRegressionUDTF$AROWe2
   * @group regression
   */
  @scala.annotation.varargs
  def train_arowe2_regr(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "train_arowe2_regr",
        new HiveFunctionWrapper("hivemall.regression.AROWRegressionUDTF$AROWe2"),
        setMixServs(toHivemallFeatureDf(exprs: _*)).map(_.expr)
      ),
      join = false, outer = false, None,
      Seq("feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.regression.LogressUDTF
   * @group regression
   */
  @scala.annotation.varargs
  def train_logregr(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "train_logregr",
        new HiveFunctionWrapper("hivemall.regression.LogressUDTF"),
        setMixServs(toHivemallFeatureDf(exprs: _*)).map(_.expr)
      ),
      join = false, outer = false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.regression.PassiveAggressiveRegressionUDTF
   * @group regression
   */
  @scala.annotation.varargs
  def train_pa1_regr(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "train_pa1_regr",
        new HiveFunctionWrapper("hivemall.regression.PassiveAggressiveRegressionUDTF"),
        setMixServs(toHivemallFeatureDf(exprs: _*)).map(_.expr)
      ),
      join = false, outer = false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.regression.PassiveAggressiveRegressionUDTF.PA1a
   * @group regression
   */
  @scala.annotation.varargs
  def train_pa1a_regr(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "train_pa1a_regr",
        new HiveFunctionWrapper("hivemall.regression.PassiveAggressiveRegressionUDTF$PA1a"),
        setMixServs(toHivemallFeatureDf(exprs: _*)).map(_.expr)
      ),
      join = false, outer = false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.regression.PassiveAggressiveRegressionUDTF.PA2
   * @group regression
   */
  @scala.annotation.varargs
  def train_pa2_regr(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "train_pa2_regr",
        new HiveFunctionWrapper("hivemall.regression.PassiveAggressiveRegressionUDTF$PA2"),
        setMixServs(toHivemallFeatureDf(exprs: _*)).map(_.expr)
      ),
      join = false, outer = false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.regression.PassiveAggressiveRegressionUDTF.PA2a
   * @group regression
   */
  @scala.annotation.varargs
  def train_pa2a_regr(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "train_pa2a_regr",
        new HiveFunctionWrapper("hivemall.regression.PassiveAggressiveRegressionUDTF$PA2a"),
        setMixServs(toHivemallFeatureDf(exprs: _*)).map(_.expr)
      ),
      join = false, outer = false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.smile.regression.RandomForestRegressionUDTF
   * @group regression
   */
  @scala.annotation.varargs
  def train_randomforest_regr(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "train_randomforest_regr",
        new HiveFunctionWrapper("hivemall.smile.regression.RandomForestRegressionUDTF"),
        setMixServs(toHivemallFeatureDf(exprs: _*)).map(_.expr)
      ),
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
  def train_perceptron(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "train_perceptron",
        new HiveFunctionWrapper("hivemall.classifier.PerceptronUDTF"),
        setMixServs(toHivemallFeatureDf(exprs: _*)).map(_.expr)
      ),
      join = false, outer = false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.PassiveAggressiveUDTF
   * @group classifier
   */
  @scala.annotation.varargs
  def train_pa(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "train_pa",
        new HiveFunctionWrapper("hivemall.classifier.PassiveAggressiveUDTF"),
        setMixServs(toHivemallFeatureDf(exprs: _*)).map(_.expr)
      ),
      join = false, outer = false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.PassiveAggressiveUDTF$PA1
   * @group classifier
   */
  @scala.annotation.varargs
  def train_pa1(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "train_pa1",
        new HiveFunctionWrapper("hivemall.classifier.PassiveAggressiveUDTF$PA1"),
        setMixServs(toHivemallFeatureDf(exprs: _*)).map(_.expr)
      ),
      join = false, outer = false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.PassiveAggressiveUDTF$PA2
   * @group classifier
   */
  @scala.annotation.varargs
  def train_pa2(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "train_pa2",
        new HiveFunctionWrapper("hivemall.classifier.PassiveAggressiveUDTF$PA2"),
        setMixServs(toHivemallFeatureDf(exprs: _*)).map(_.expr)
      ),
      join = false, outer = false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.ConfidenceWeightedUDTF
   * @group classifier
   */
  @scala.annotation.varargs
  def train_cw(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "train_cw",
        new HiveFunctionWrapper("hivemall.classifier.ConfidenceWeightedUDTF"),
        setMixServs(toHivemallFeatureDf(exprs: _*)).map(_.expr)
      ),
      join = false, outer = false, None,
      Seq("feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.AROWClassifierUDTF
   * @group classifier
   */
  @scala.annotation.varargs
  def train_arow(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "train_arow",
        new HiveFunctionWrapper("hivemall.classifier.AROWClassifierUDTF"),
        setMixServs(toHivemallFeatureDf(exprs: _*)).map(_.expr)
      ),
      join = false, outer = false, None,
      Seq("feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.AROWClassifierUDTF$AROWh
   * @group classifier
   */
  @scala.annotation.varargs
  def train_arowh(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "train_arowh",
        new HiveFunctionWrapper("hivemall.classifier.AROWClassifierUDTF$AROWh"),
        setMixServs(toHivemallFeatureDf(exprs: _*)).map(_.expr)
      ),
      join = false, outer = false, None,
      Seq("feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.SoftConfideceWeightedUDTF$SCW1
   * @group classifier
   */
  @scala.annotation.varargs
  def train_scw(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "train_scw",
        new HiveFunctionWrapper("hivemall.classifier.SoftConfideceWeightedUDTF$SCW1"),
        setMixServs(toHivemallFeatureDf(exprs: _*)).map(_.expr)
      ),
      join = false, outer = false, None,
      Seq("feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.SoftConfideceWeightedUDTF$SCW1
   * @group classifier
   */
  @scala.annotation.varargs
  def train_scw2(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "train_scw2",
        new HiveFunctionWrapper("hivemall.classifier.SoftConfideceWeightedUDTF$SCW2"),
        setMixServs(toHivemallFeatureDf(exprs: _*)).map(_.expr)
      ),
      join = false, outer = false, None,
      Seq("feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.classifier.AdaGradRDAUDTF
   * @group classifier
   */
  @scala.annotation.varargs
  def train_adagrad_rda(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
       "train_adagrad_rda",
        new HiveFunctionWrapper("hivemall.classifier.AdaGradRDAUDTF"),
        setMixServs(toHivemallFeatureDf(exprs: _*)).map(_.expr)
      ),
      join = false, outer = false, None,
      Seq("feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.smile.classification.RandomForestClassifierUDTF
   * @group classifier
   */
  @scala.annotation.varargs
  def train_randomforest_classifier(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
       "train_randomforest_classifier",
        new HiveFunctionWrapper("hivemall.smile.classification.RandomForestClassifierUDTF"),
        setMixServs(toHivemallFeatureDf(exprs: _*)).map(_.expr)
      ),
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
  def train_multiclass_perceptron(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "train_multiclass_perceptron",
        new HiveFunctionWrapper("hivemall.classifier.multiclass.MulticlassPerceptronUDTF"),
        setMixServs(toHivemallFeatureDf(exprs: _*)).map(_.expr)
      ),
      join = false, outer = false, None,
      Seq("label", "feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.classifier.PassiveAggressiveUDTF
   * @group classifier.multiclass
   */
  @scala.annotation.varargs
  def train_multiclass_pa(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "train_multiclass_pa",
        new HiveFunctionWrapper("hivemall.classifier.multiclass.MulticlassPassiveAggressiveUDTF"),
        setMixServs(toHivemallFeatureDf(exprs: _*)).map(_.expr)
      ),
      join = false, outer = false, None,
      Seq("label", "feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.classifier.PassiveAggressiveUDTF$PA1
   * @group classifier.multiclass
   */
  @scala.annotation.varargs
  def train_multiclass_pa1(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "train_multiclass_pa1",
        new HiveFunctionWrapper(
          "hivemall.classifier.multiclass.MulticlassPassiveAggressiveUDTF$PA1"),
        setMixServs(toHivemallFeatureDf(exprs: _*)).map(_.expr)
      ),
      join = false, outer = false, None,
      Seq("label", "feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.classifier.PassiveAggressiveUDTF$PA2
   * @group classifier.multiclass
   */
  @scala.annotation.varargs
  def train_multiclass_pa2(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "train_multiclass_pa2",
        new HiveFunctionWrapper(
          "hivemall.classifier.multiclass.MulticlassPassiveAggressiveUDTF$PA2"),
        setMixServs(toHivemallFeatureDf(exprs: _*)).map(_.expr)
      ),
      join = false, outer = false, None,
      Seq("label", "feature", "weight").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.classifier.MulticlassConfidenceWeightedUDTF
   * @group classifier.multiclass
   */
  @scala.annotation.varargs
  def train_multiclass_cw(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "train_multiclass_cw",
        new HiveFunctionWrapper("hivemall.classifier.multiclass.MulticlassConfidenceWeightedUDTF"),
        setMixServs(toHivemallFeatureDf(exprs: _*)).map(_.expr)
      ),
      join = false, outer = false, None,
      Seq("label", "feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.classifier.MulticlassAROWClassifierUDTF
   * @group classifier.multiclass
   */
  @scala.annotation.varargs
  def train_multiclass_arow(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "train_multiclass_arow",
        new HiveFunctionWrapper("hivemall.classifier.multiclass.MulticlassAROWClassifierUDTF"),
        setMixServs(toHivemallFeatureDf(exprs: _*)).map(_.expr)
      ),
      join = false, outer = false, None,
      Seq("label", "feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.classifier.MulticlassSoftConfidenceWeightedUDTF$SCW1
   * @group classifier.multiclass
   */
  @scala.annotation.varargs
  def train_multiclass_scw(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "train_multiclass_scw",
        new HiveFunctionWrapper(
          "hivemall.classifier.multiclass.MulticlassSoftConfidenceWeightedUDTF$SCW1"),
        setMixServs(toHivemallFeatureDf(exprs: _*)).map(_.expr)
      ),
      join = false, outer = false, None,
      Seq("label", "feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * @see hivemall.classifier.classifier.MulticlassSoftConfidenceWeightedUDTF$SCW2
   * @group classifier.multiclass
   */
  @scala.annotation.varargs
  def train_multiclass_scw2(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "train_multiclass_scw2",
        new HiveFunctionWrapper(
          "hivemall.classifier.multiclass.MulticlassSoftConfidenceWeightedUDTF$SCW2"
        ),
        setMixServs(toHivemallFeatureDf(exprs: _*)).map(_.expr)
      ),
      join = false, outer = false, None,
      Seq("label", "feature", "weight", "conv").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * :: Experimental ::
   * @see hivemall.xgboost.regression.XGBoostRegressionUDTF
   * @group xgboost
   */
  @Experimental
  @scala.annotation.varargs
  def train_xgboost_regr(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "train_xgboost_regr",
        new HiveFunctionWrapper("hivemall.xgboost.regression.XGBoostRegressionUDTFWrapper"),
        toHivemallFeatureDf(exprs : _*).map(_.expr)
      ),
      join = false, outer = false, None,
      Seq("model_id", "pred_model").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * :: Experimental ::
   * @see hivemall.xgboost.classification.XGBoostBinaryClassifierUDTF
   * @group xgboost
   */
  @Experimental
  @scala.annotation.varargs
  def train_xgboost_classifier(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "train_xgboost_classifier",
        new HiveFunctionWrapper(
          "hivemall.xgboost.classification.XGBoostBinaryClassifierUDTFWrapper"),
        toHivemallFeatureDf(exprs : _*).map(_.expr)
      ),
      join = false, outer = false, None,
      Seq("model_id", "pred_model").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * :: Experimental ::
   * @see hivemall.xgboost.classification.XGBoostMulticlassClassifierUDTF
   * @group xgboost
   */
  @Experimental
  @scala.annotation.varargs
  def train_xgboost_multiclass_classifier(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "train_xgboost_multiclass_classifier",
        new HiveFunctionWrapper(
          "hivemall.xgboost.classification.XGBoostMulticlassClassifierUDTFWrapper"
        ),
        toHivemallFeatureDf(exprs: _*).map(_.expr)
      ),
      join = false, outer = false, None,
      Seq("model_id", "pred_model").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * :: Experimental ::
   * @see hivemall.xgboost.tools.XGBoostPredictUDTF
   * @group xgboost
   */
  @Experimental
  @scala.annotation.varargs
  def xgboost_predict(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "xgboost_predict",
        new HiveFunctionWrapper("hivemall.xgboost.tools.XGBoostPredictUDTF"),
        toHivemallFeatureDf(exprs: _*).map(_.expr)
      ),
      join = false, outer = false, None,
      Seq("rowid", "predicted").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * :: Experimental ::
   * @see hivemall.xgboost.tools.XGBoostMulticlassPredictUDTF
   * @group xgboost
   */
  @Experimental
  @scala.annotation.varargs
  def xgboost_multiclass_predict(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "xgboost_multiclass_predict",
        new HiveFunctionWrapper("hivemall.xgboost.tools.XGBoostMulticlassPredictUDTF"),
        toHivemallFeatureDf(exprs: _*).map(_.expr)
      ),
      join = false, outer = false, None,
      Seq("rowid", "label", "probability").map(UnresolvedAttribute(_)),
      df.logicalPlan)
  }

  /**
   * Groups the [[DataFrame]] using the specified columns, so we can run aggregation on them.
   * See [[RelationalGroupedDatasetEx]] for all the available aggregate functions.
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
  def groupby(cols: Column*): RelationalGroupedDatasetEx = {
    new RelationalGroupedDatasetEx(df, cols.map(_.expr), RelationalGroupedDataset.GroupByType)
  }

  @scala.annotation.varargs
  def groupby(col1: String, cols: String*): RelationalGroupedDatasetEx = {
    val colNames: Seq[String] = col1 +: cols
    new RelationalGroupedDatasetEx(df, colNames.map(colName => df(colName).expr),
      RelationalGroupedDataset.GroupByType)
  }

  /**
   * @see hivemall.knn.lsh.MinHashUDTF
   * @group knn.lsh
   */
  @scala.annotation.varargs
  def minhash(exprs: Column*): DataFrame = withTypedPlan {
     Generate(HiveGenericUDTF(
        "minhash",
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
  def amplify(exprs: Column*): DataFrame = withTypedPlan {
    val outputAttr = exprs.drop(1).map {
      case Column(expr: NamedExpression) => UnresolvedAttribute(expr.name)
      case Column(expr: Expression) => UnresolvedAttribute(expr.simpleString)
    }
    Generate(HiveGenericUDTF(
        "amplify",
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
  def rand_amplify(exprs: Column*): DataFrame = withTypedPlan {
    throw new UnsupportedOperationException("`rand_amplify` not supported yet")
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
  def quantify(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "quantify",
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
  def binarize_label(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "binarize_label",
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
  def quantified_features(exprs: Column*): DataFrame = withTypedPlan {
    Generate(HiveGenericUDTF(
        "quantified_features",
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

  /**
   * Splits `org.apache.spark.ml.linalg.Vector` into pieces.
   * @group ftvec
   */
  def explode_vector(expr: Column): DataFrame = {
    val elementSchema = StructType(
      StructField("feature", StringType) :: StructField("weight", DoubleType) :: Nil)
    val explodeFunc: Row => TraversableOnce[InternalRow] = (row: Row) => {
      row.get(0) match {
        case dv: SDV =>
          dv.values.zipWithIndex.map {
            case (value, index) =>
              InternalRow(UTF8String.fromString(s"$index"), value)
          }
        case sv: SSV =>
          sv.values.zip(sv.indices).map {
            case (value, index) =>
              InternalRow(UTF8String.fromString(s"$index"), value)
          }
      }
    }
    withTypedPlan {
      Generate(
        UserDefinedGenerator(elementSchema, explodeFunc, expr.expr :: Nil),
        join = true, outer = false, None,
        generatorOutput = Nil,
        df.logicalPlan)
    }
  }

  /**
   * Returns `top-k` records for each `group`.
   * @group misc
   * @since 0.5.0
   */
  def each_top_k(k: Int, group: String, score: String, args: String*)
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

  @deprecated("use each_top_k(Int, String, String, String*) instead", "0.5.0")
  def each_top_k(k: Column, group: Column, value: Column, args: Column*): DataFrame = {
    val kInt = k.expr match {
      case Literal(v: Any, IntegerType) => v.asInstanceOf[Int]
      case e => throw new AnalysisException("`k` must be integer, however " + e)
    }
    val groupStr = usePrettyExpression(group.expr).sql
    val valueStr = usePrettyExpression(value.expr).sql
    val argStrs = args.map(c => usePrettyExpression(c.expr).sql)
    each_top_k(kInt, groupStr, valueStr, argStrs: _*)
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
  def lr_datagen(exprs: Column*): Dataset[Row] = withTypedPlan {
    Generate(HiveGenericUDTF(
        "lr_datagen",
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
  @Experimental
  private[this] def setMixServs(exprs: Seq[Column]): Seq[Column] = {
    val mixes = System.getenv("HIVEMALL_MIX_SERVERS")
    if (mixes != null && !mixes.isEmpty()) {
      val groupId = df.sqlContext.sparkContext.applicationId + "-" + UUID.randomUUID
      logInfo(s"set '${mixes}' as default mix servers (session: ${groupId})")
      exprs.size match {
        case 2 => exprs :+ Column(
          Literal.create(s"-mix ${mixes} -mix_session ${groupId}", StringType))
        /** TODO: Add codes in the case where exprs.size == 3. */
        case _ => exprs
      }
    } else {
      exprs
    }
  }

  @inline private[this] def toHivemallFeatureDf(exprs: Column*): Seq[Column] = {
    df.select(exprs: _*).queryExecution.analyzed.schema.zip(exprs).map {
      case (StructField(_, _: VectorUDT, _, _), c) => to_hivemall_features(c)
      case (_, c) => c
    }
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

object HivemallOps {

  /**
   * Implicitly inject the [[HivemallOps]] into [[DataFrame]].
   */
  implicit def dataFrameToHivemallOps(df: DataFrame): HivemallOps =
    new HivemallOps(df)

  /**
   * @see hivemall.HivemallVersionUDF
   * @group misc
   */
  def hivemall_version(): Column = withExpr {
    HiveSimpleUDF("hivemall_version", new HiveFunctionWrapper("hivemall.HivemallVersionUDF"), Nil)
  }

  /**
   * @see hivemall.knn.similarity.CosineSimilarityUDF
   * @group knn.similarity
   */
  @scala.annotation.varargs
  def cosine_sim(exprs: Column*): Column = withExpr {
    HiveGenericUDF("cosine_sim",
      new HiveFunctionWrapper("hivemall.knn.similarity.CosineSimilarityUDF"),
      exprs.map(_.expr))
  }

  /**
   * @see hivemall.knn.similarity.JaccardIndexUDF
   * @group knn.similarity
   */
  @scala.annotation.varargs
  def jaccard(exprs: Column*): Column = withExpr {
    HiveSimpleUDF("jaccard",
      new HiveFunctionWrapper("hivemall.knn.similarity.JaccardIndexUDF"),
      exprs.map(_.expr))
  }

  /**
   * @see hivemall.knn.similarity.AngularSimilarityUDF
   * @group knn.similarity
   */
  @scala.annotation.varargs
  def angular_similarity(exprs: Column*): Column = withExpr {
    HiveGenericUDF("angular_similarity",
      new HiveFunctionWrapper("hivemall.knn.similarity.AngularSimilarityUDF"),
      exprs.map(_.expr))
  }

  /**
   * @see hivemall.knn.similarity.EuclidSimilarity
   * @group knn.similarity
   */
  @scala.annotation.varargs
  def euclid_similarity(exprs: Column*): Column = withExpr {
    HiveGenericUDF("euclid_similarity",
      new HiveFunctionWrapper("hivemall.knn.similarity.EuclidSimilarity"),
      exprs.map(_.expr))
  }

  /**
   * @see hivemall.knn.similarity.Distance2SimilarityUDF
   * @group knn.similarity
   */
  @scala.annotation.varargs
  def distance2similarity(exprs: Column*): Column = withExpr {
    // TODO: Need a wrapper class because of using unsupported types
    HiveGenericUDF("distance2similarity",
      new HiveFunctionWrapper("hivemall.knn.similarity.Distance2SimilarityUDF"),
      exprs.map(_.expr))
  }

  /**
   * @see hivemall.knn.distance.HammingDistanceUDF
   * @group knn.distance
   */
  @scala.annotation.varargs
  def hamming_distance(exprs: Column*): Column = withExpr {
    HiveSimpleUDF("hamming_distance",
      new HiveFunctionWrapper("hivemall.knn.distance.HammingDistanceUDF"),
      exprs.map(_.expr))
  }

  /**
   * @see hivemall.knn.distance.PopcountUDF
   * @group knn.distance
   */
  @scala.annotation.varargs
  def popcnt(exprs: Column*): Column = withExpr {
    HiveSimpleUDF("popcnt",
      new HiveFunctionWrapper("hivemall.knn.distance.PopcountUDF"),
      exprs.map(_.expr))
  }

  /**
   * @see hivemall.knn.distance.KLDivergenceUDF
   * @group knn.distance
   */
  @scala.annotation.varargs
  def kld(exprs: Column*): Column = withExpr {
    HiveSimpleUDF("kld",
      new HiveFunctionWrapper("hivemall.knn.distance.KLDivergenceUDF"),
      exprs.map(_.expr))
  }

  /**
   * @see hivemall.knn.distance.EuclidDistanceUDF
   * @group knn.distance
   */
  @scala.annotation.varargs
  def euclid_distance(exprs: Column*): Column = withExpr {
    HiveGenericUDF("euclid_distance",
      new HiveFunctionWrapper("hivemall.knn.distance.EuclidDistanceUDF"),
      exprs.map(_.expr))
  }

  /**
   * @see hivemall.knn.distance.CosineDistanceUDF
   * @group knn.distance
   */
  @scala.annotation.varargs
  def cosine_distance(exprs: Column*): Column = withExpr {
    HiveGenericUDF("cosine_distance",
      new HiveFunctionWrapper("hivemall.knn.distance.CosineDistanceUDF"),
      exprs.map(_.expr))
  }

  /**
   * @see hivemall.knn.distance.AngularDistanceUDF
   * @group knn.distance
   */
  @scala.annotation.varargs
  def angular_distance(exprs: Column*): Column = withExpr {
    HiveGenericUDF("angular_distance",
      new HiveFunctionWrapper("hivemall.knn.distance.AngularDistanceUDF"),
      exprs.map(_.expr))
  }

  /**
   * @see hivemall.knn.distance.ManhattanDistanceUDF
   * @group knn.distance
   */
  @scala.annotation.varargs
  def manhattan_distance(exprs: Column*): Column = withExpr {
    HiveGenericUDF("manhattan_distance",
      new HiveFunctionWrapper("hivemall.knn.distance.ManhattanDistanceUDF"),
      exprs.map(_.expr))
  }

  /**
   * @see hivemall.knn.distance.MinkowskiDistanceUDF
   * @group knn.distance
   */
  @scala.annotation.varargs
  def minkowski_distance (exprs: Column*): Column = withExpr {
    HiveGenericUDF("minkowski_distance",
      new HiveFunctionWrapper("hivemall.knn.distance.MinkowskiDistanceUDF"),
      exprs.map(_.expr))
  }

  /**
   * @see hivemall.knn.lsh.bBitMinHashUDF
   * @group knn.lsh
   */
  @scala.annotation.varargs
  def bbit_minhash(exprs: Column*): Column = withExpr {
    HiveSimpleUDF("bbit_minhash",
      new HiveFunctionWrapper("hivemall.knn.lsh.bBitMinHashUDF"),
      exprs.map(_.expr))
  }

  /**
   * @see hivemall.knn.lsh.MinHashesUDF
   * @group knn.lsh
   */
  @scala.annotation.varargs
  def minhashes(exprs: Column*): Column = withExpr {
    HiveGenericUDF("minhashes",
      new HiveFunctionWrapper("hivemall.knn.lsh.MinHashesUDFWrapper"),
      exprs.map(_.expr))
  }

  /**
   * Returns new features with `1.0` (bias) appended to the input features.
   * @group ftvec
   */
  def add_bias(expr: Column): Column = withExpr {
    HiveGenericUDF("add_bias",
      new HiveFunctionWrapper("hivemall.ftvec.AddBiasUDFWrapper"),
      expr.expr :: Nil)
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
      "extract_feature",
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
      "extract_weight",
      new HiveFunctionWrapper("hivemall.ftvec.ExtractWeightUDFWrapper"),
      expr.expr :: Nil)
    Column(hiveUdf).as("value")
  }

  /**
   * @see hivemall.ftvec.AddFeatureIndexUDFWrapper
   * @group ftvec
   */
  def add_feature_index(expr: Column): Column = withExpr {
    HiveGenericUDF("add_feature_index",
      new HiveFunctionWrapper("hivemall.ftvec.AddFeatureIndexUDFWrapper"),
      expr.expr :: Nil)
  }

  /**
   * @see hivemall.ftvec.SortByFeatureUDF
   * @group ftvec
   */
  def sort_by_feature(expr: Column): Column = withExpr {
    HiveGenericUDF("sort_by_feature",
      new HiveFunctionWrapper("hivemall.ftvec.SortByFeatureUDFWrapper"),
      expr.expr :: Nil)
  }

  /**
   * @see hivemall.ftvec.hashing.MurmurHash3UDF
   * @group ftvec.hashing
   */
  def mhash(expr: Column): Column = withExpr {
    HiveSimpleUDF("mhash",
      new HiveFunctionWrapper("hivemall.ftvec.hashing.MurmurHash3UDF"),
      expr.expr :: Nil)
  }

  /**
   * @see hivemall.ftvec.hashing.Sha1UDF
   * @group ftvec.hashing
   */
  def sha1(expr: Column): Column = withExpr {
    HiveSimpleUDF("sha1",
      new HiveFunctionWrapper("hivemall.ftvec.hashing.Sha1UDF"),
      expr.expr :: Nil)
  }

  /**
   * @see hivemall.ftvec.hashing.ArrayHashValuesUDF
   * @group ftvec.hashing
   */
  @scala.annotation.varargs
  def array_hash_values(exprs: Column*): Column = withExpr {
    // TODO: Need a wrapper class because of using unsupported types
    HiveSimpleUDF("array_hash_values",
      new HiveFunctionWrapper("hivemall.ftvec.hashing.ArrayHashValuesUDF"),
      exprs.map(_.expr))
  }

  /**
   * @see hivemall.ftvec.hashing.ArrayPrefixedHashValuesUDF
   * @group ftvec.hashing
   */
  @scala.annotation.varargs
  def prefixed_hash_values(exprs: Column*): Column = withExpr {
    // TODO: Need a wrapper class because of using unsupported types
    HiveSimpleUDF("prefixed_hash_values",
      new HiveFunctionWrapper("hivemall.ftvec.hashing.ArrayPrefixedHashValuesUDF"),
      exprs.map(_.expr))
  }

  /**
   * @see hivemall.ftvec.scaling.RescaleUDF
   * @group ftvec.scaling
   */
  def rescale(value: Column, max: Column, min: Column): Column = withExpr {
    HiveSimpleUDF("rescale",
      new HiveFunctionWrapper("hivemall.ftvec.scaling.RescaleUDF"),
      (value.cast(FloatType) :: max :: min :: Nil).map(_.expr))
  }

  /**
   * @see hivemall.ftvec.scaling.ZScoreUDF
   * @group ftvec.scaling
   */
  @scala.annotation.varargs
  def zscore(exprs: Column*): Column = withExpr {
    HiveSimpleUDF("zscore",
      new HiveFunctionWrapper("hivemall.ftvec.scaling.ZScoreUDF"),
      exprs.map(_.expr))
  }

  /**
   * @see hivemall.ftvec.scaling.L2NormalizationUDF
   * @group ftvec.scaling
   */
  def normalize(expr: Column): Column = withExpr {
    HiveGenericUDF("normalize",
      new HiveFunctionWrapper("hivemall.ftvec.scaling.L2NormalizationUDFWrapper"),
      expr.expr :: Nil)
  }

  /**
   * @see hivemall.ftvec.conv.ToDenseFeaturesUDF
   * @group ftvec.conv
   */
  @scala.annotation.varargs
  def to_dense_features(exprs: Column*): Column = withExpr {
    // TODO: Need a wrapper class because of using unsupported types
    HiveGenericUDF("to_dense_features",
      new HiveFunctionWrapper("hivemall.ftvec.conv.ToDenseFeaturesUDF"),
      exprs.map(_.expr))
  }

  /**
   * @see hivemall.ftvec.conv.ToSparseFeaturesUDF
   * @group ftvec.conv
   */
  @scala.annotation.varargs
  def to_sparse_features(exprs: Column*): Column = withExpr {
    // TODO: Need a wrapper class because of using unsupported types
    HiveGenericUDF("to_sparse_features",
      new HiveFunctionWrapper("hivemall.ftvec.conv.ToSparseFeaturesUDF"),
      exprs.map(_.expr))
  }

  /**
   * @see hivemall.ftvec.trans.VectorizeFeaturesUDF
   * @group ftvec.trans
   */
  @scala.annotation.varargs
  def vectorize_features(exprs: Column*): Column = withExpr {
    HiveGenericUDF("vectorize_features",
      new HiveFunctionWrapper("hivemall.ftvec.trans.VectorizeFeaturesUDF"),
      exprs.map(_.expr))
  }

  /**
   * @see hivemall.ftvec.trans.CategoricalFeaturesUDF
   * @group ftvec.trans
   */
  @scala.annotation.varargs
  def categorical_features(exprs: Column*): Column = withExpr {
    HiveGenericUDF("categorical_features",
      new HiveFunctionWrapper("hivemall.ftvec.trans.CategoricalFeaturesUDF"),
      exprs.map(_.expr))
  }

  /**
   * @see hivemall.ftvec.trans.IndexedFeatures
   * @group ftvec.trans
   */
  @scala.annotation.varargs
  def indexed_features(exprs: Column*): Column = withExpr {
    HiveGenericUDF("indexed_features",
      new HiveFunctionWrapper("hivemall.ftvec.trans.IndexedFeatures"),
      exprs.map(_.expr))
  }

  /**
   * @see hivemall.ftvec.trans.QuantitativeFeaturesUDF
   * @group ftvec.trans
   */
  @scala.annotation.varargs
  def quantitative_features(exprs: Column*): Column = withExpr {
    HiveGenericUDF("quantitative_features",
      new HiveFunctionWrapper("hivemall.ftvec.trans.QuantitativeFeaturesUDF"),
      exprs.map(_.expr))
  }

  /**
   * @see hivemall.smile.tools.TreePredictUDF
   * @group misc
   */
  @scala.annotation.varargs
  def tree_predict(exprs: Column*): Column = withExpr {
    HiveGenericUDF("tree_predict",
      new HiveFunctionWrapper("hivemall.smile.tools.TreePredictUDF"),
      exprs.map(_.expr))
  }

  /**
   * @see hivemall.tools.math.SigmoidUDF
   * @group misc
   */
  def sigmoid(expr: Column): Column = {
    val one: () => Literal = () => Literal.create(1.0, DoubleType)
    Column(one()) / (Column(one()) + exp(-expr))
  }

  /**
   * @see hivemall.tools.mapred.RowIdUDF
   * @group misc
   */
  def rowid(): Column = withExpr {
    HiveGenericUDF("rowid", new HiveFunctionWrapper("hivemall.tools.mapred.RowIdUDFWrapper"), Nil)
  }.as("rowid")

  /**
   * A convenient function to wrap an expression and produce a Column.
   */
  @inline private def withExpr(expr: Expression): Column = Column(expr)
}
