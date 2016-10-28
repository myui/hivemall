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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

@SuppressWarnings("deprecation")
@Description(name = "logloss",
        value = "_FUNC_(double predicted, double actual) - Return a Logrithmic Loss")
public final class LogarithmicLossUDAF extends UDAF {

    public static class Evaluator implements UDAFEvaluator {

        private PartialResult partial;

        public Evaluator() {}

        @Override
        public void init() {
            this.partial = null;
        }

        public boolean iterate(DoubleWritable predicted, DoubleWritable actual)
                throws HiveException {
            if (predicted == null || actual == null) {// skip
                return true;
            }
            if (partial == null) {
                this.partial = new PartialResult();
            }
            partial.iterate(predicted.get(), actual.get());
            return true;
        }

        public PartialResult terminatePartial() {
            return partial;
        }

        public boolean merge(PartialResult other) throws HiveException {
            if (other == null) {
                return true;
            }
            if (partial == null) {
                this.partial = new PartialResult();
            }
            partial.merge(other);
            return true;
        }

        public double terminate() {
            if (partial == null) {
                return 0.d;
            }
            return partial.getLogLoss();
        }
    }

    public static class PartialResult {

        double log_sum;
        long count;

        PartialResult() {
            this.log_sum = 0.d;
            this.count = 0L;
        }

        void iterate(double predicted, double actual) {
            double epsilon = 1E-15d;
            predicted = Math.max(epsilon, predicted);
            predicted = Math.min(1.d - epsilon, predicted);
            log_sum += actual * Math.log(predicted) + (1.d - actual) * Math.log(1.d - predicted);
            count++;
        }

        void merge(PartialResult other) {
            log_sum += other.log_sum;
            count += other.count;
        }

        double getLogLoss() {
            if (count == 0) {
                return 0.d;
            }
            return -1.d * log_sum / count;
        }

    }

}
