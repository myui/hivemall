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
import org.apache.spark.sql.types._
import org.apache.spark.util.BoundedPriorityQueue

case class EachTopK(k: Int, children: Seq[Expression]) extends Generator with CodegenFallback {

  private def getKey(d: (Double, InternalRow)) = d._1

  private[this] val queue: BoundedPriorityQueue[(Double, InternalRow)] =
    new BoundedPriorityQueue(k)(Ordering.by(getKey))
  private[this] var prevGroup: Int = -1

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.size > 1 && children(1).dataType == DoubleType) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(
        "the second column must have double-typed data, "
          + s"however: ${children(1).dataType}")
    }
  }

  override def elementSchema: StructType =
    StructType(
      Seq(StructField("rank", IntegerType)) ++
        children.map(d => StructField(d.prettyName, d.dataType))
    )

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    val ret = if (prevGroup != input.getInt(0) && queue.size > 0) {
      val iter = queue.iterator.toSeq.sortBy(_._1)(Ordering[Double].reverse)
          .zipWithIndex.map { case ((_, row), index) =>
        new JoinedRow(InternalRow(index), row)
      }
      queue.clear()
      iter
    } else {
      Seq.empty[InternalRow]
    }
    queue += Tuple2(input.getDouble(1), input.copy())
    prevGroup = input.getInt(0)
    ret
  }

  override def terminate(): TraversableOnce[InternalRow] = {
    if (queue.size > 0) {
      val d = queue.iterator.toSeq.sortBy(_._1)(Ordering[Double].reverse)
          .zipWithIndex.map { case ((_, row), index) =>
        new JoinedRow(InternalRow(index), row)
      }.toSeq
      queue.clear()
      d
    } else {
      Iterator.empty
    }
  }
}
