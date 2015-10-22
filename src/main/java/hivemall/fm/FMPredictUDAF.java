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
package hivemall.fm;

import hivemall.utils.lang.mutable.MutableDouble;

import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;

@Description(name = "fm_predict", value = "_FUNC_(Float Wj, array<float> Vjf, float Xj) - Returns a prediction value")
public final class FMPredictUDAF extends UDAF {

    public FMPredictUDAF() {}

    public static class Evaluator implements UDAFEvaluator {

        private PartialResult partial;

        public Evaluator() {}

        @Override
        public void init() {
            this.partial = null;
        }

        public boolean iterate(@Nullable DoubleWritable Wj, @Nullable List<FloatWritable> Vjf, @Nullable DoubleWritable Xj)
                throws HiveException {
            if(partial == null) {
                this.partial = new PartialResult();
            }
            partial.iterate(Wj, Vjf, Xj);
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

        public DoubleWritable terminate() {
            if(partial == null) {
                return null;
            }
            double result = partial.getPrediction();
            return new DoubleWritable(result);
        }

    }

    private static final class PartialResult {
        private double ret;
        // note that primitive array cannot be serialized by JDK serializer
        private List<MutableDouble> sumVjXj;
        private List<MutableDouble> sumV2X2;

        PartialResult() {
            this.ret = 0.d;
            this.sumVjXj = null;
            this.sumV2X2 = null;
        }

        void iterate(@Nullable DoubleWritable Wj, @Nullable List<FloatWritable> Vjf, @Nullable DoubleWritable Xj)
                throws HiveException {
            if(Wj != null) {
                if(Xj == null) {// W0
                    this.ret += Wj.get();
                    return;
                } else {// Wj (j>=1)
                    this.ret += (Wj.get() * Xj.get());
                }
            }
            if(Vjf != null) {
                if(Xj == null) {
                    throw new HiveException("Xj should not be null");
                }
                if(sumVjXj == null) {
                    int factors = Vjf.size();
                    this.sumVjXj = Arrays.asList(MutableDouble.initArray(factors, 0.d));
                    this.sumV2X2 = Arrays.asList(MutableDouble.initArray(factors, 0.d));
                }

                final double x = Xj.get();
                final int factor = Vjf.size();
                if(factor < 1) {
                    throw new HiveException("# of Factor should be more than 0: " + Vjf.toString());
                }
                for(int f = 0; f < factor; f++) {
                    FloatWritable v = Vjf.get(f);
                    if(v == null) {
                        throw new HiveException("Vj" + f + " should not be null");
                    }
                    double vx = v.get() * x;
                    MutableDouble sumVXf = sumVjXj.get(f);
                    sumVXf.addValue(vx);
                    MutableDouble sumVX2f = sumV2X2.get(f);
                    sumVX2f.addValue(vx * vx);
                }
            }
        }

        void merge(PartialResult other) {
            this.ret += other.ret;
            if(this.sumVjXj == null) {
                this.sumVjXj = other.sumVjXj;
                this.sumV2X2 = other.sumV2X2;
            } else {
                add(other.sumVjXj, sumVjXj);
                add(other.sumV2X2, sumV2X2);
            }
        }

        double getPrediction() {
            if(sumVjXj == null) {
                return ret;
            }

            final int factor = sumVjXj.size();
            for(int f = 0; f < factor; f++) {
                MutableDouble v1 = sumVjXj.get(f);
                assert (v1 != null);
                double d1 = v1.doubleValue();
                MutableDouble v2 = sumV2X2.get(f);
                assert (v2 != null);
                double d2 = v2.doubleValue();
                ret += 0.5d * (d1 * d1 - d2);
            }
            return ret;
        }

        private static void add(@Nonnull final List<MutableDouble> src, @Nonnull final List<MutableDouble> dst) {
            for(int i = 0, size = src.size(); i < size; i++) {
                MutableDouble s = src.get(i);
                assert (s != null);
                MutableDouble d = dst.get(i);
                assert (d != null);
                d.addValue(s.getValue());
            }
        }

    }

}
