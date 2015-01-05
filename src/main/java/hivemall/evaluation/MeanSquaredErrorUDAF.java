/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013-2014
 *   National Institute of Advanced Industrial Science and Technology (AIST)
 *   Registration Number: H25PRO-1520
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package hivemall.evaluation;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

@Description(name = "mse", value = "_FUNC_(predicted, actual) - Return a Mean Squared Error")
public final class MeanSquaredErrorUDAF extends UDAF {

    public static class Evaluator implements UDAFEvaluator {

        private PartialResult partial;

        public Evaluator() {}

        @Override
        public void init() {
            this.partial = null;
        }

        public boolean iterate(DoubleWritable predicted, DoubleWritable actual)
                throws HiveException {
            if(predicted == null || actual == null) {// skip
                return true;
            }
            if(partial == null) {
                this.partial = new PartialResult();
            }
            partial.iterate(predicted.get(), actual.get());
            return true;
        }

        public PartialResult terminatePartial() {
            return partial;
        }

        public boolean merge(PartialResult other) throws HiveException {
            if(other == null) {
                return true;
            }
            if(partial == null) {
                this.partial = new PartialResult();
            }
            partial.merge(other);
            return true;
        }

        public double terminate() {
            if(partial == null) {
                return 0.d;
            }
            return partial.getMSE();
        }
    }

    public static class PartialResult {

        double diff_sum;
        long count;

        PartialResult() {
            this.diff_sum = 0d;
            this.count = 0L;
        }

        void iterate(double predicted, double actual) {
            diff_sum += Math.pow(predicted - actual, 2.d);
            count++;
        }

        void merge(PartialResult other) {
            diff_sum += other.diff_sum;
            count += other.count;
        }

        double getMSE() {
            return diff_sum / count;
        }

    }

}
