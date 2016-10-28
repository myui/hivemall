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
package hivemall.tools.array;

import hivemall.utils.lang.ArrayUtils;

import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

@SuppressWarnings("deprecation")
@Description(name = "array_sum", value = "_FUNC_(array<number>) - Returns an array<double>"
        + " in which each element is summed up")
public final class ArraySumUDAF extends UDAF {

    public ArraySumUDAF() {}

    public static class Evaluator implements UDAFEvaluator {

        private PartialResult partial;

        public Evaluator() {}

        @Override
        public void init() {
            this.partial = null;
        }

        public boolean iterate(List<Double> tuple) throws HiveException {
            if (tuple == null) {
                return true;
            }
            if (tuple.isEmpty()) {// a special case
                return true;
            }
            final int size = tuple.size();
            if (partial == null) {
                this.partial = new PartialResult(size);
            }
            partial.update(tuple);
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
                this.partial = new PartialResult(other._size);
            }
            partial.merge(other);
            return true;
        }

        public List<DoubleWritable> terminate() {
            if (partial == null) {
                return null;
            }

            final int size = partial._size;
            final List<Double> sum = partial._sum;

            final DoubleWritable[] ary = new DoubleWritable[size];
            for (int i = 0; i < size; i++) {
                Double d = sum.get(i);
                ary[i] = new DoubleWritable(d.doubleValue());
            }
            return Arrays.asList(ary);
        }
    }

    public static class PartialResult {

        int _size;
        // note that primitive array cannot be serialized by JDK serializer
        List<Double> _sum;

        public PartialResult() {}

        PartialResult(int size) throws HiveException {
            assert (size > 0) : size;
            this._size = size;
            this._sum = ArrayUtils.toList(new double[size]);
        }

        void update(@Nonnull final List<Double> tuple) throws HiveException {
            if (tuple.size() != _size) {// a corner case
                throw new HiveException("Mismatch in the number of elements at tuple: "
                        + tuple.toString());
            }
            final List<Double> sum = _sum;
            for (int i = 0, len = _size; i < len; i++) {
                Double v = tuple.get(i);
                if (v != null) {
                    double d = sum.get(i).doubleValue() + v.doubleValue();
                    sum.set(i, Double.valueOf(d));
                }
            }
        }

        void merge(PartialResult other) throws HiveException {
            if (other._size != _size) {
                throw new HiveException("Mismatch in the number of elements");
            }
            final List<Double> sum = _sum, o_sum = other._sum;
            for (int i = 0, len = _size; i < len; i++) {
                double d = sum.get(i).doubleValue() + o_sum.get(i).doubleValue();
                sum.set(i, Double.valueOf(d));
            }
        }
    }

}
