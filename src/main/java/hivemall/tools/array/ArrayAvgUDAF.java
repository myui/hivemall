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
package hivemall.tools.array;

import hivemall.utils.lang.ArrayUtils;

import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;

public final class ArrayAvgUDAF extends UDAF {

    private ArrayAvgUDAF() {}//prevent instantiation

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

        public List<Float> terminate() {
            if(partial == null) {
                return null;
            }

            final int size = partial._size;
            final List<Double> sum = partial._sum;
            final List<Long> count = partial._count;

            final Float[] ary = new Float[size];
            for(int i = 0; i < size; i++) {
                long c = count.get(i);
                float avg = (c == 0) ? 0.f : (float) (sum.get(i) / c);
                ary[i] = Float.valueOf(avg);
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
