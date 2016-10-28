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
package hivemall.evaluation;

import hivemall.utils.hadoop.WritableUtils;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

@SuppressWarnings("deprecation")
@Description(name = "f1score",
        value = "_FUNC_(array[int], array[int]) - Return a F-measure/F1 score")
public final class FMeasureUDAF extends UDAF {

    public static class Evaluator implements UDAFEvaluator {

        public static class PartialResult {
            long tp;
            /** tp + fn */
            long totalAcutal;
            /** tp + fp */
            long totalPredicted;

            PartialResult() {
                this.tp = 0L;
                this.totalPredicted = 0L;
                this.totalAcutal = 0L;
            }

            void updateScore(final List<IntWritable> actual, final List<IntWritable> predicted) {
                final int numActual = actual.size();
                final int numPredicted = predicted.size();
                int countTp = 0;
                for (int i = 0; i < numPredicted; i++) {
                    IntWritable p = predicted.get(i);
                    if (actual.contains(p)) {
                        countTp++;
                    }
                }
                this.tp += countTp;
                this.totalAcutal += numActual;
                this.totalPredicted += numPredicted;
            }

            void merge(PartialResult other) {
                this.tp = other.tp;
                this.totalAcutal = other.totalAcutal;
                this.totalPredicted = other.totalPredicted;
            }
        }

        private PartialResult partial;

        @Override
        public void init() {
            this.partial = null;
        }

        public boolean iterate(List<IntWritable> actual, List<IntWritable> predicted) {
            if (partial == null) {
                this.partial = new PartialResult();
            }
            partial.updateScore(actual, predicted);
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
            }
            partial.merge(other);
            return true;
        }

        public DoubleWritable terminate() {
            if (partial == null) {
                return null;
            }
            double score = f1Score(partial);
            return WritableUtils.val(score);
        }

        /**
         * @return 2 * precision * recall / (precision + recall)
         */
        private static double f1Score(final PartialResult partial) {
            double precision = precision(partial);
            double recall = recall(partial);
            double divisor = precision + recall;
            if (divisor > 0) {
                return (2.d * precision * recall) / divisor;
            } else {
                return -1d;
            }
        }

        private static double precision(final PartialResult partial) {
            return (partial.totalPredicted == 0L) ? 0d : partial.tp
                    / (double) partial.totalPredicted;
        }

        private static double recall(final PartialResult partial) {
            return (partial.totalAcutal == 0L) ? 0d : partial.tp / (double) partial.totalAcutal;
        }

    }
}
