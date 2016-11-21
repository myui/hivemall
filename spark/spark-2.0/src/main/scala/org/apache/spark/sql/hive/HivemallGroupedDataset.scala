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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.RelationalGroupedDataset
import org.apache.spark.sql.catalyst.analysis.UnresolvedAlias
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.catalyst.plans.logical.Pivot
import org.apache.spark.sql.hive.HiveShim.HiveFunctionWrapper
import org.apache.spark.sql.types._

/**
 * Groups the [[DataFrame]] using the specified columns, so we can run aggregation on them.
 *
 * @groupname ensemble
 * @groupname ftvec.trans
 * @groupname evaluation
 */
final class HivemallGroupedDataset(groupBy: RelationalGroupedDataset) {

  /**
   * @see hivemall.ensemble.bagging.VotedAvgUDAF
   * @group ensemble
   */
  def voted_avg(weight: String): DataFrame = {
    // checkType(weight, NumericType)
    val udaf = HiveUDAFFunction(
        "voted_avg",
        new HiveFunctionWrapper("hivemall.ensemble.bagging.WeightVotedAvgUDAF"),
        Seq(weight).map(df.col(_).expr),
        isUDAFBridgeRequired = true)
      .toAggregateExpression()
    toDF((Alias(udaf, udaf.prettyName)() :: Nil).toSeq)
  }

  /**
   * @see hivemall.ensemble.bagging.WeightVotedAvgUDAF
   * @group ensemble
   */
  def weight_voted_avg(weight: String): DataFrame = {
    // checkType(weight, NumericType)
    val udaf = HiveUDAFFunction(
        "weight_voted_avg",
        new HiveFunctionWrapper("hivemall.ensemble.bagging.WeightVotedAvgUDAF"),
        Seq(weight).map(df.col(_).expr),
        isUDAFBridgeRequired = true)
      .toAggregateExpression()
    toDF((Alias(udaf, udaf.prettyName)() :: Nil).toSeq)
  }

  /**
   * @see hivemall.ensemble.ArgminKLDistanceUDAF
   * @group ensemble
   */
  def argmin_kld(weight: String, conv: String): DataFrame = {
    // checkType(weight, NumericType)
    // checkType(conv, NumericType)
    val udaf = HiveUDAFFunction(
        "argmin_kld",
        new HiveFunctionWrapper("hivemall.ensemble.ArgminKLDistanceUDAF"),
        Seq(weight, conv).map(df.col(_).expr),
        isUDAFBridgeRequired = true)
      .toAggregateExpression()
    toDF((Alias(udaf, udaf.prettyName)() :: Nil).toSeq)
  }

  /**
   * @see hivemall.ensemble.MaxValueLabelUDAF"
   * @group ensemble
   */
  def max_label(score: String, label: String): DataFrame = {
    // checkType(score, NumericType)
    checkType(label, StringType)
    val udaf = HiveUDAFFunction(
        "max_label",
        new HiveFunctionWrapper("hivemall.ensemble.MaxValueLabelUDAF"),
        Seq(score, label).map(df.col(_).expr),
        isUDAFBridgeRequired = true)
      .toAggregateExpression()
    toDF((Alias(udaf, udaf.prettyName)() :: Nil).toSeq)
  }

  /**
   * @see hivemall.ensemble.MaxRowUDAF
   * @group ensemble
   */
  def maxrow(score: String, label: String): DataFrame = {
    // checkType(score, NumericType)
    checkType(label, StringType)
    val udaf = HiveUDAFFunction(
        "maxrow",
        new HiveFunctionWrapper("hivemall.ensemble.MaxRowUDAF"),
        Seq(score, label).map(df.col(_).expr),
        isUDAFBridgeRequired = false)
      .toAggregateExpression()
    toDF((Alias(udaf, udaf.prettyName)() :: Nil).toSeq)
  }

  /**
   * @see hivemall.smile.tools.RandomForestEnsembleUDAF
   * @group ensemble
   */
  def rf_ensemble(predict: String): DataFrame = {
    // checkType(predict, NumericType)
    val udaf = HiveUDAFFunction(
        "rf_ensemble",
        new HiveFunctionWrapper("hivemall.smile.tools.RandomForestEnsembleUDAF"),
        Seq(predict).map(df.col(_).expr),
        isUDAFBridgeRequired = true)
      .toAggregateExpression()
    toDF((Alias(udaf, udaf.prettyName)() :: Nil).toSeq)
  }

  /**
   * @see hivemall.ftvec.trans.OnehotEncodingUDAF
   * @group ftvec.trans
   */
  def onehot_encoding(cols: String*): DataFrame = {
    val udaf = HiveUDAFFunction(
        "onehot_encoding",
        new HiveFunctionWrapper("hivemall.ftvec.trans.OnehotEncodingUDAF"),
        cols.map(df.col(_).expr),
        isUDAFBridgeRequired = false)
      .toAggregateExpression()
    toDF(Seq(Alias(udaf, udaf.prettyName)()))
  }

  /**
   * @see hivemall.evaluation.MeanAbsoluteErrorUDAF
   * @group evaluation
   */
  def mae(predict: String, target: String): DataFrame = {
    checkType(predict, FloatType)
    checkType(target, FloatType)
    val udaf = HiveUDAFFunction(
        "mae",
        new HiveFunctionWrapper("hivemall.evaluation.MeanAbsoluteErrorUDAF"),
        Seq(predict, target).map(df.col(_).expr),
        isUDAFBridgeRequired = true)
      .toAggregateExpression()
    toDF((Alias(udaf, udaf.prettyName)() :: Nil).toSeq)
  }

  /**
   * @see hivemall.evaluation.MeanSquareErrorUDAF
   * @group evaluation
   */
  def mse(predict: String, target: String): DataFrame = {
    checkType(predict, FloatType)
    checkType(target, FloatType)
    val udaf = HiveUDAFFunction(
        "mse",
        new HiveFunctionWrapper("hivemall.evaluation.MeanSquaredErrorUDAF"),
        Seq(predict, target).map(df.col(_).expr),
        isUDAFBridgeRequired = true)
      .toAggregateExpression()
    toDF((Alias(udaf, udaf.prettyName)() :: Nil).toSeq)
  }

  /**
   * @see hivemall.evaluation.RootMeanSquareErrorUDAF
   * @group evaluation
   */
  def rmse(predict: String, target: String): DataFrame = {
    checkType(predict, FloatType)
    checkType(target, FloatType)
    val udaf = HiveUDAFFunction(
      "rmse",
      new HiveFunctionWrapper("hivemall.evaluation.RootMeanSquaredErrorUDAF"),
        Seq(predict, target).map(df.col(_).expr),
        isUDAFBridgeRequired = true)
      .toAggregateExpression()
    toDF((Alias(udaf, udaf.prettyName)() :: Nil).toSeq)
  }

  /**
   * @see hivemall.evaluation.FMeasureUDAF
   * @group evaluation
   */
  def f1score(predict: String, target: String): DataFrame = {
    // checkType(target, ArrayType(IntegerType))
    // checkType(predict, ArrayType(IntegerType))
    val udaf = HiveUDAFFunction(
        "f1score",
        new HiveFunctionWrapper("hivemall.evaluation.FMeasureUDAF"),
        Seq(predict, target).map(df.col(_).expr),
        isUDAFBridgeRequired = true)
      .toAggregateExpression()
    toDF((Alias(udaf, udaf.prettyName)() :: Nil).toSeq)
  }

  /**
   * [[RelationalGroupedDataset]] has the three values as private fields, so, to inject Hivemall
   * aggregate functions, we fetch them via Java Reflections.
   */
  private val df = getPrivateField[DataFrame]("org$apache$spark$sql$RelationalGroupedDataset$$df")
  private val groupingExprs = getPrivateField[Seq[Expression]]("groupingExprs")
  private val groupType = getPrivateField[RelationalGroupedDataset.GroupType]("groupType")

  private def getPrivateField[T](name: String): T = {
    val field = groupBy.getClass.getDeclaredField(name)
    field.setAccessible(true)
    field.get(groupBy).asInstanceOf[T]
  }

  private def toDF(aggExprs: Seq[Expression]): DataFrame = {
    val aggregates = if (df.sqlContext.conf.dataFrameRetainGroupColumns) {
      groupingExprs ++ aggExprs
    } else {
      aggExprs
    }

    val aliasedAgg = aggregates.map(alias)

    groupType match {
      case RelationalGroupedDataset.GroupByType =>
        Dataset.ofRows(
          df.sparkSession, Aggregate(groupingExprs, aliasedAgg, df.logicalPlan))
      case RelationalGroupedDataset.RollupType =>
        Dataset.ofRows(
          df.sparkSession, Aggregate(Seq(Rollup(groupingExprs)), aliasedAgg, df.logicalPlan))
      case RelationalGroupedDataset.CubeType =>
        Dataset.ofRows(
          df.sparkSession, Aggregate(Seq(Cube(groupingExprs)), aliasedAgg, df.logicalPlan))
      case RelationalGroupedDataset.PivotType(pivotCol, values) =>
        val aliasedGrps = groupingExprs.map(alias)
        Dataset.ofRows(
          df.sparkSession, Pivot(aliasedGrps, pivotCol, values, aggExprs, df.logicalPlan))
    }
  }

  private def alias(expr: Expression): NamedExpression = expr match {
    case u: UnresolvedAttribute => UnresolvedAlias(u)
    case expr: NamedExpression => expr
    case expr: Expression => Alias(expr, expr.prettyName)()
  }

  private def checkType(colName: String, expected: DataType) = {
    val dataType = df.resolve(colName).dataType
    if (dataType != expected) {
      throw new AnalysisException(
        s""""$colName" must be $expected, however it is $dataType""")
    }
  }
}

object HivemallGroupedDataset {

  /**
   * Implicitly inject the [[HivemallGroupedDataset]] into [[RelationalGroupedDataset]].
   */
  implicit def relationalGroupedDatasetToHivemallOne(
      groupBy: RelationalGroupedDataset): HivemallGroupedDataset = {
    new HivemallGroupedDataset(groupBy)

  /**
   * @see hivemall.ftvec.selection.SignalNoiseRatioUDAF
   */
  def snr(X: String, Y: String): DataFrame = {
    val udaf = HiveUDAFFunction(
        "snr",
        new HiveFunctionWrapper("hivemall.ftvec.selection.SignalNoiseRatioUDAF"),
        Seq(X, Y).map(df.col(_).expr),
        isUDAFBridgeRequired = false)
      .toAggregateExpression()
    toDF(Seq(Alias(udaf, udaf.prettyName)()))
  }

  /**
   * @see hivemall.tools.matrix.TransposeAndDotUDAF
   */
  def transpose_and_dot(X: String, Y: String): DataFrame = {
    val udaf = HiveUDAFFunction(
        "transpose_and_dot",
        new HiveFunctionWrapper("hivemall.tools.matrix.TransposeAndDotUDAF"),
        Seq(X, Y).map(df.col(_).expr),
        isUDAFBridgeRequired = false)
      .toAggregateExpression()
    toDF(Seq(Alias(udaf, udaf.prettyName)()))
  }
}
