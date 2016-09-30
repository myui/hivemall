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

import org.apache.spark.sql.{AnalysisException, DataFrame, GroupedData}
import org.apache.spark.sql.catalyst.analysis.{Star, UnresolvedAlias, UnresolvedAttribute, UnresolvedFunction}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Cube, Rollup, Pivot}
import org.apache.spark.sql.hive.HiveShim.HiveFunctionWrapper
import org.apache.spark.sql.types._

final class GroupedDataEx protected[sql](
    df: DataFrame,
    groupingExprs: Seq[Expression],
    private val groupType: GroupedData.GroupType)
  extends GroupedData(df, groupingExprs, groupType) {

  //////////////////////////////////////////////////////////////////////////////////////////////
  // toDF, alias, and strToExpr are copyed from the base class, GroupedData, because
  // these methods have 'private[this]' modifiers.
  //////////////////////////////////////////////////////////////////////////////////////////////

  private[this] def toDF(aggExprs: Seq[Expression]): DataFrame = {
    val aggregates = if (df.sqlContext.conf.dataFrameRetainGroupColumns) {
      groupingExprs ++ aggExprs
    } else {
      aggExprs
    }

    val aliasedAgg = aggregates.map(alias)

    groupType match {
      case GroupedData.GroupByType =>
        DataFrame(
          df.sqlContext, Aggregate(groupingExprs, aliasedAgg, df.logicalPlan))
      case GroupedData.RollupType =>
        DataFrame(
          df.sqlContext, Rollup(groupingExprs, df.logicalPlan, aliasedAgg))
      case GroupedData.CubeType =>
        DataFrame(
          df.sqlContext, Cube(groupingExprs, df.logicalPlan, aliasedAgg))
      case GroupedData.PivotType(pivotCol, values) =>
        val aliasedGrps = groupingExprs.map(alias)
        DataFrame(
          df.sqlContext, Pivot(aliasedGrps, pivotCol, values, aggExprs, df.logicalPlan))
    }
  }

  private[this] def alias(expr: Expression): NamedExpression = expr match {
    case u: UnresolvedAttribute => UnresolvedAlias(u)
    case expr: NamedExpression => expr
    case expr: Expression => Alias(expr, expr.prettyString)()
  }

  private[this] def strToExpr(expr: String): (Expression => Expression) = {
    val exprToFunc: (Expression => Expression) = {
      (inputExpr: Expression) => expr.toLowerCase match {
        case "avg" | "average" | "mean" =>
          UnresolvedFunction("avg", inputExpr :: Nil, isDistinct = false)
        case "stddev" | "std" =>
          UnresolvedFunction("stddev", inputExpr :: Nil, isDistinct = false)
        // Also special handle count because we need to take care count(*).
        case "count" | "size" =>
          // Turn count(*) into count(1)
          inputExpr match {
            case s: Star => Count(Literal(1)).toAggregateExpression()
            case _ => Count(inputExpr).toAggregateExpression()
          }
        case name => UnresolvedFunction(name, inputExpr :: Nil, isDistinct = false)
      }
    }
    (inputExpr: Expression) => exprToFunc(inputExpr)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////

  // `agg` only supports aggregation functions with a single argument
  override def agg(exprs: Map[String, String]): DataFrame = {
    toDF(exprs.map { case (colName, expr) =>
      val a = expr match {
        case "voted_avg" =>
          HiveUDAFFunction(
            new HiveFunctionWrapper("hivemall.ensemble.bagging.VotedAvgUDAF"),
            Seq(df.col(colName).expr),
            isUDAFBridgeRequired = true
          ).toAggregateExpression
        case "weight_voted_avg" =>
          HiveUDAFFunction(
            new HiveFunctionWrapper("hivemall.ensemble.bagging.WeightVotedAvgUDAF"),
            Seq(df.col(colName).expr),
            isUDAFBridgeRequired = true
          ).toAggregateExpression()
        case "rf_ensemble" =>
          HiveUDAFFunction(
            new HiveFunctionWrapper("hivemall.smile.tools.RandomForestEnsembleUDAF"),
            Seq(df.col(colName).expr),
            isUDAFBridgeRequired = true
          ).toAggregateExpression()
        case _ =>
          strToExpr(expr)(df(colName).expr)
      }
      Alias(a, a.prettyString)()
    }.toSeq)
  }

  private[this] def checkType(colName: String, expected: DataType) = {
    val dataType = df.resolve(colName).dataType
    if (dataType != expected) {
      throw new AnalysisException(
        s""""$colName" must be $expected, however it is $dataType""")
    }
  }

  /**
   * @see hivemall.ensemble.bagging.VotedAvgUDAF
   */
  def voted_avg(weight: String): DataFrame = {
    // checkType(weight, NumericType)
    val udaf = HiveUDAFFunction(
        new HiveFunctionWrapper("hivemall.ensemble.bagging.WeightVotedAvgUDAF"),
        Seq(weight).map(df.col(_).expr),
        isUDAFBridgeRequired = true)
      .toAggregateExpression()
    toDF((Alias(udaf, udaf.prettyString)() :: Nil).toSeq)
  }

  /**
   * @see hivemall.ensemble.bagging.WeightVotedAvgUDAF
   */
  def weight_voted_avg(weight: String): DataFrame = {
    // checkType(weight, NumericType)
    val udaf = HiveUDAFFunction(
        new HiveFunctionWrapper("hivemall.ensemble.bagging.WeightVotedAvgUDAF"),
        Seq(weight).map(df.col(_).expr),
        isUDAFBridgeRequired = true)
      .toAggregateExpression()
    toDF((Alias(udaf, udaf.prettyString)() :: Nil).toSeq)
  }

  /**
   * @see hivemall.ensemble.ArgminKLDistanceUDAF
   */
  def argmin_kld(weight: String, conv: String): DataFrame = {
    // checkType(weight, NumericType)
    // checkType(conv, NumericType)
    val udaf = HiveUDAFFunction(
        new HiveFunctionWrapper("hivemall.ensemble.ArgminKLDistanceUDAF"),
        Seq(weight, conv).map(df.col(_).expr),
        isUDAFBridgeRequired = true)
      .toAggregateExpression()
    toDF((Alias(udaf, udaf.prettyString)() :: Nil).toSeq)
  }

  /**
   * @see hivemall.ensemble.MaxValueLabelUDAF"
   */
  def max_label(score: String, label: String): DataFrame = {
    // checkType(score, NumericType)
    checkType(label, StringType)
    val udaf = HiveUDAFFunction(
        new HiveFunctionWrapper("hivemall.ensemble.MaxValueLabelUDAF"),
        Seq(score, label).map(df.col(_).expr),
        isUDAFBridgeRequired = true)
      .toAggregateExpression()
    toDF((Alias(udaf, udaf.prettyString)() :: Nil).toSeq)
  }

  /**
   * @see hivemall.ensemble.MaxRowUDAF
   */
  def maxrow(score: String, label: String): DataFrame = {
    // checkType(score, NumericType)
    checkType(label, StringType)
    val udaf = HiveUDAFFunction(
        new HiveFunctionWrapper("hivemall.ensemble.MaxRowUDAF"),
        Seq(score, label).map(df.col(_).expr),
        isUDAFBridgeRequired = false)
      .toAggregateExpression()
    toDF((Alias(udaf, udaf.prettyString)() :: Nil).toSeq)
  }

  /**
   * @see hivemall.smile.tools.RandomForestEnsembleUDAF
   */
  def rf_ensemble(predict: String): DataFrame = {
    // checkType(predict, NumericType)
    val udaf = HiveUDAFFunction(
        new HiveFunctionWrapper("hivemall.smile.tools.RandomForestEnsembleUDAF"),
        Seq(predict).map(df.col(_).expr),
        isUDAFBridgeRequired = true)
      .toAggregateExpression()
    toDF((Alias(udaf, udaf.prettyString)() :: Nil).toSeq)
  }

  /**
   * @see hivemall.evaluation.MeanAbsoluteErrorUDAF
   */
  def mae(predict: String, target: String): DataFrame = {
    checkType(predict, FloatType)
    checkType(target, FloatType)
    val udaf = HiveUDAFFunction(
        new HiveFunctionWrapper("hivemall.evaluation.MeanAbsoluteErrorUDAF"),
        Seq(predict, target).map(df.col(_).expr),
        isUDAFBridgeRequired = true)
      .toAggregateExpression()
    toDF((Alias(udaf, udaf.prettyString)() :: Nil).toSeq)
  }

  /**
   * @see hivemall.evaluation.MeanSquareErrorUDAF
   */
  def mse(predict: String, target: String): DataFrame = {
    checkType(predict, FloatType)
    checkType(target, FloatType)
    val udaf = HiveUDAFFunction(
        new HiveFunctionWrapper("hivemall.evaluation.MeanSquaredErrorUDAF"),
        Seq(predict, target).map(df.col(_).expr),
        isUDAFBridgeRequired = true)
      .toAggregateExpression()
    toDF((Alias(udaf, udaf.prettyString)() :: Nil).toSeq)
  }

  /**
   * @see hivemall.evaluation.RootMeanSquareErrorUDAF
   */
  def rmse(predict: String, target: String): DataFrame = {
    checkType(predict, FloatType)
    checkType(target, FloatType)
    val udaf = HiveUDAFFunction(
      new HiveFunctionWrapper("hivemall.evaluation.RootMeanSquaredErrorUDAF"),
        Seq(predict, target).map(df.col(_).expr),
        isUDAFBridgeRequired = true)
      .toAggregateExpression()
    toDF((Alias(udaf, udaf.prettyString)() :: Nil).toSeq)
  }

  /**
   * @see hivemall.evaluation.FMeasureUDAF
   */
  def f1score(predict: String, target: String): DataFrame = {
    // checkType(target, ArrayType(IntegerType))
    // checkType(predict, ArrayType(IntegerType))
    val udaf = HiveUDAFFunction(
        new HiveFunctionWrapper("hivemall.evaluation.FMeasureUDAF"),
        Seq(predict, target).map(df.col(_).expr),
        isUDAFBridgeRequired = true)
      .toAggregateExpression()
    toDF((Alias(udaf, udaf.prettyString)() :: Nil).toSeq)
  }

  /**
   * @see hivemall.ftvec.selection.SignalNoiseRatioUDAF
   */
  def snr(X: String, Y: String): DataFrame = {
    val udaf = HiveUDAFFunction(
      new HiveFunctionWrapper("hivemall.ftvec.selection.SignalNoiseRatioUDAF"),
      Seq(X, Y).map(df.col(_).expr),
      isUDAFBridgeRequired = false)
      .toAggregateExpression()
    toDF(Seq(Alias(udaf, udaf.prettyString)()))
  }

  /**
   * @see hivemall.tools.matrix.TransposeAndDotUDAF
   */
  def transpose_and_dot(X: String, Y: String): DataFrame = {
    val udaf = HiveUDAFFunction(
      new HiveFunctionWrapper("hivemall.tools.matrix.TransposeAndDotUDAF"),
      Seq(X, Y).map(df.col(_).expr),
      isUDAFBridgeRequired = false)
      .toAggregateExpression()
    toDF(Seq(Alias(udaf, udaf.prettyString)()))
  }
}
