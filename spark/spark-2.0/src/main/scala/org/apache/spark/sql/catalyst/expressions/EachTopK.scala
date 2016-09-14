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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._
import org.apache.spark.util.BoundedPriorityQueue

case class EachTopK(
    k: Int,
    groupingExpression: Expression,
    scoreExpression: Expression,
    children: Seq[Attribute]) extends Generator with CodegenFallback {
  type QueueType = (AnyRef, InternalRow)

  require(k != 0, "`k` must not have 0")

  private[this] lazy val scoreType = scoreExpression.dataType
  private[this] lazy val scoreOrdering = {
    val ordering = TypeUtils.getInterpretedOrdering(scoreType)
      .asInstanceOf[Ordering[AnyRef]]
    if (k > 0) {
      ordering
    } else {
      ordering.reverse
    }
  }
  private[this] lazy val reverseScoreOrdering = scoreOrdering.reverse

  private[this] val queue: BoundedPriorityQueue[QueueType] = {
    new BoundedPriorityQueue(Math.abs(k))(new Ordering[QueueType] {
      override def compare(x: QueueType, y: QueueType): Int =
        scoreOrdering.compare(x._1, y._1)
    })
  }

  lazy private[this] val groupingProjection: UnsafeProjection =
    UnsafeProjection.create(groupingExpression :: Nil, children)

  lazy private[this] val scoreProjection: UnsafeProjection =
    UnsafeProjection.create(scoreExpression :: Nil, children)

  // The grouping key of the current partition
  private[this] var currentGroupingKey: UnsafeRow = _

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!TypeCollection.Ordered.acceptsType(scoreExpression.dataType)) {
      TypeCheckResult.TypeCheckFailure(
        s"$scoreExpression must have a comparable type")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def elementSchema: StructType =
    StructType(
      Seq(StructField("rank", IntegerType)) ++
        children.map(d => StructField(d.prettyName, d.dataType))
    )

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    val groupingKey = groupingProjection(input)
    val ret = if (currentGroupingKey != groupingKey) {
      val part = queue.iterator.toSeq.sortBy(_._1)(reverseScoreOrdering)
          .zipWithIndex.map { case ((_, row), index) =>
        new JoinedRow(InternalRow(1 + index), row)
      }
      currentGroupingKey = groupingKey.copy()
      queue.clear()
      part
    } else {
      Iterator.empty
    }
    queue += Tuple2(scoreProjection(input).get(0, scoreType), input.copy())
    ret
  }

  override def terminate(): TraversableOnce[InternalRow] = {
    if (queue.size > 0) {
      val part = queue.iterator.toSeq.sortBy(_._1)(reverseScoreOrdering)
          .zipWithIndex.map { case ((_, row), index) =>
        new JoinedRow(InternalRow(1 + index), row)
      }
      queue.clear()
      part
    } else {
      Iterator.empty
    }
  }
}
