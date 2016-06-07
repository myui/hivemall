/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
 * Copyright (C) 2013-2015 National Institute of Advanced Industrial Science and Technology (AIST)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hivemall.evaluation;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

@Description(
        name = "r2",
        value = "_FUNC_(double predicted, double actual) - Return R Squared (coefficient of determination)")
public final class R2UDAF extends UDAF {

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
            return partial.getR2();
        }
    }

    public static class PartialResult {

        double residual_sum_of_squares;
        List<Double> actuals;
        double sum_actuals;
        long count;

        PartialResult() {
            this.residual_sum_of_squares = 0.d;
            this.actuals = new ArrayList<Double>();
            this.sum_actuals = 0.d;
            this.count = 0L;
        }

        void iterate(double predicted, double actual) {
            this.residual_sum_of_squares += Math.pow(actual - predicted, 2.d);
            this.actuals.add(actual);
            this.sum_actuals += actual;
            this.count++;
        }

        void merge(PartialResult other) {
            residual_sum_of_squares += other.residual_sum_of_squares;
            this.actuals.addAll(other.actuals);
            this.sum_actuals += other.sum_actuals;
            count += other.count;
        }

        double getR2() {
            double avg_actuals = this.sum_actuals / this.count;
            double total_sum_of_squares = 0.d;

            for (Double a : actuals) {
                total_sum_of_squares += Math.pow(a - avg_actuals, 2.d);
            }

            if (total_sum_of_squares == 0.d) {
                return 1.d;
            }
            return 1.d - this.residual_sum_of_squares / total_sum_of_squares;
        }

    }

}
