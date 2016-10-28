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
package hivemall.ensemble.bagging;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

@SuppressWarnings("deprecation")
@Description(name = "voted_avg",
        value = "_FUNC_(double value) - Returns an averaged value by bagging for classification")
public final class VotedAvgUDAF extends UDAF {

    public static class Evaluator implements UDAFEvaluator {

        public static class PartialResult {

            double positiveSum;
            int positiveCnt;
            double negativeSum;
            int negativeCnt;

            void init() {
                positiveSum = 0.d;
                positiveCnt = 0;
                negativeSum = 0.d;
                negativeCnt = 0;
            }

        }

        private PartialResult partial;

        @Override
        public void init() {
            this.partial = null;
        }

        public boolean iterate(double w) {
            if (partial == null) {
                this.partial = new PartialResult();
                partial.init();
            }
            if (w > 0) {
                partial.positiveSum += w;
                partial.positiveCnt++;
            } else if (w < 0) {
                partial.negativeSum += w;
                partial.negativeCnt++;
            }
            return true;
        }

        public PartialResult terminatePartial() {
            return partial;
        }

        public boolean merge(PartialResult other) {
            if (other == null) {
                return true;
            }
            if (partial == null) {
                this.partial = new PartialResult();
                partial.init();
            }
            partial.positiveSum += other.positiveSum;
            partial.positiveCnt += other.positiveCnt;
            partial.negativeSum += other.negativeSum;
            partial.negativeCnt += other.negativeCnt;
            return true;
        }

        public DoubleWritable terminate() {
            if (partial == null) {
                return null; // null to indicate that no values have been aggregated yet
            }
            if (partial.positiveCnt > partial.negativeCnt) {
                return new DoubleWritable(partial.positiveSum / partial.positiveCnt);
            } else {
                if (partial.negativeCnt == 0) {
                    assert (partial.negativeSum == 0.d) : partial.negativeSum;
                    return new DoubleWritable(0.d);
                }
                return new DoubleWritable(partial.negativeSum / partial.negativeCnt);
            }
        }
    }
}
