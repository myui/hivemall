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
package hivemall.tools.array;

import hivemall.utils.lang.ArrayUtils;

import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.FloatWritable;

@Deprecated
@Description(name = "array_avg", value = "_FUNC_(array) - Returns an array<double>"
        + " in which each element is the mean of a set of numbers")
public final class ArrayAvgUDAF extends UDAF {

    public ArrayAvgUDAF() {}

    public static class Evaluator implements UDAFEvaluator {

        private PartialResult partial;

        public Evaluator() {}

        @Override
        public void init() {
            this.partial = null;
        }

        public boolean iterate(List<Double> tuple) throws HiveException {
            if(tuple == null) {
                return true;
            }
            if(tuple.isEmpty()) {// a special case
                return true;
            }
            final int size = tuple.size();
            if(partial == null) {
                this.partial = new PartialResult(size);
            }
            partial.update(tuple);
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
                this.partial = new PartialResult(other._size);
            }
            partial.merge(other);
            return true;
        }

        public List<FloatWritable> terminate() {
            if(partial == null) {
                return null;
            }

            final int size = partial._size;
            final List<Double> sum = partial._sum;
            final List<Long> count = partial._count;

            final FloatWritable[] ary = new FloatWritable[size];
            for(int i = 0; i < size; i++) {
                long c = count.get(i);
                float avg = (c == 0) ? 0.f : (float) (sum.get(i) / c);
                ary[i] = new FloatWritable(avg);
            }
            return Arrays.asList(ary);
        }
    }

    public static class PartialResult {

        int _size;
        // note that primitive array cannot be serialized by JDK serializer
        List<Double> _sum;
        List<Long> _count;

        public PartialResult() {}

        PartialResult(int size) throws HiveException {
            assert (size > 0) : size;
            this._size = size;
            this._sum = ArrayUtils.toList(new double[size]);
            this._count = ArrayUtils.toList(new long[size]);
        }

        void update(@Nonnull final List<Double> tuple) throws HiveException {
            if(tuple.size() != _size) {// a corner case
                throw new HiveException("Mismatch in the number of elements at tuple: "
                        + tuple.toString());
            }
            final List<Double> sum = _sum;
            final List<Long> count = _count;
            for(int i = 0, len = _size; i < len; i++) {
                Double v = tuple.get(i);
                if(v != null) {
                    sum.set(i, sum.get(i) + v.doubleValue());
                    count.set(i, count.get(i) + 1);
                }
            }
        }

        void merge(PartialResult other) throws HiveException {
            if(other._size != _size) {
                throw new HiveException("Mismatch in the number of elements");
            }
            final List<Double> sum = _sum, o_sum = other._sum;
            final List<Long> count = _count, o_count = other._count;
            for(int i = 0, len = _size; i < len; i++) {
                sum.set(i, sum.get(i) + o_sum.get(i));
                count.set(i, count.get(i) + o_count.get(i));
            }
        }
    }

}
