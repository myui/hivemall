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

import scala.reflect.runtime.{universe => ru}

import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame

object TestUtils extends Logging {

  // Do benchmark if INFO-log enabled
  def benchmark(benchName: String)(testFunc: => Unit): Unit = {
    if (log.isDebugEnabled) {
      testFunc
    }
  }

  def expectResult(res: Boolean, errMsg: String) = if (res) {
    logWarning(errMsg)
  }

  def invokeFunc(cls: Any, func: String, args: Any*): DataFrame = try {
    // Invoke a function with the given name via reflection
    val im = scala.reflect.runtime.currentMirror.reflect(cls)
    val mSym = im.symbol.typeSignature.member(ru.newTermName(func)).asMethod
    im.reflectMethod(mSym).apply(args: _*)
      .asInstanceOf[DataFrame]
  } catch {
    case e: Exception =>
      assert(false, s"Invoking ${func} failed because: ${e.getMessage}")
      null // Not executed
  }
}

// TODO: Any same function in o.a.spark.*?
class TestDoubleWrapper(d: Double) {
  // Check an equality between Double values
  def ~==(d: Double): Boolean = Math.abs(this.d - d) < 0.001
}

object TestDoubleWrapper {
  @inline implicit def toTestDoubleWrapper(d: Double) = new TestDoubleWrapper(d)
}
