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
package hivemall.mf;

import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;

@Description(
        name = "mf_predict",
        value = "_FUNC_(List<Float> Pu, List<Float> Qi[, double Bu, double Bi[, double mu]]) - Returns the prediction value")
@UDFType(deterministic = true, stateful = false)
public final class MFPredictionUDF extends UDF {

    @Nonnull
    public DoubleWritable evaluate(@Nullable List<FloatWritable> Pu,
            @Nullable List<FloatWritable> Qi) throws HiveException {
        return evaluate(Pu, Qi, null);
    }

    @Nonnull
    public DoubleWritable evaluate(@Nullable List<FloatWritable> Pu,
            @Nullable List<FloatWritable> Qi, @Nullable DoubleWritable mu) throws HiveException {
        final double muValue = (mu == null) ? 0.d : mu.get();
        if (Pu == null || Qi == null) {
            return new DoubleWritable(muValue);
        }

        final int PuSize = Pu.size();
        final int QiSize = Qi.size();
        // workaround for TD
        if (PuSize == 0) {
            return new DoubleWritable(muValue);
        } else if (QiSize == 0) {
            return new DoubleWritable(muValue);
        }

        if (QiSize != PuSize) {
            throw new HiveException("|Pu| " + PuSize + " was not equal to |Qi| " + QiSize);
        }

        double ret = muValue;
        for (int k = 0; k < PuSize; k++) {
            FloatWritable Pu_k = Pu.get(k);
            if (Pu_k == null) {
                continue;
            }
            FloatWritable Qi_k = Qi.get(k);
            if (Qi_k == null) {
                continue;
            }
            ret += Pu_k.get() * Qi_k.get();
        }
        return new DoubleWritable(ret);
    }

    @Nonnull
    public DoubleWritable evaluate(@Nullable List<FloatWritable> Pu,
            @Nullable List<FloatWritable> Qi, @Nullable DoubleWritable Bu,
            @Nullable DoubleWritable Bi) throws HiveException {
        return evaluate(Pu, Qi, Bu, Bi, null);
    }

    @Nonnull
    public DoubleWritable evaluate(@Nullable List<FloatWritable> Pu,
            @Nullable List<FloatWritable> Qi, @Nullable DoubleWritable Bu,
            @Nullable DoubleWritable Bi, @Nullable DoubleWritable mu) throws HiveException {
        final double muValue = (mu == null) ? 0.d : mu.get();
        if (Pu == null && Qi == null) {
            return new DoubleWritable(muValue);
        }
        final double BiValue = (Bi == null) ? 0.d : Bi.get();
        final double BuValue = (Bu == null) ? 0.d : Bu.get();
        if (Pu == null) {
            double ret = muValue + BiValue;
            return new DoubleWritable(ret);
        } else if (Qi == null) {
            return new DoubleWritable(muValue);
        }

        final int PuSize = Pu.size();
        final int QiSize = Qi.size();
        // workaround for TD        
        if (PuSize == 0) {
            if (QiSize == 0) {
                return new DoubleWritable(muValue);
            } else {
                double ret = muValue + BiValue;
                return new DoubleWritable(ret);
            }
        } else if (QiSize == 0) {
            double ret = muValue + BuValue;
            return new DoubleWritable(ret);
        }

        if (QiSize != PuSize) {
            throw new HiveException("|Pu| " + PuSize + " was not equal to |Qi| " + QiSize);
        }

        double ret = muValue + BuValue + BiValue;
        for (int k = 0; k < PuSize; k++) {
            FloatWritable Pu_k = Pu.get(k);
            if (Pu_k == null) {
                continue;
            }
            FloatWritable Qi_k = Qi.get(k);
            if (Qi_k == null) {
                continue;
            }
            ret += Pu_k.get() * Qi_k.get();
        }
        return new DoubleWritable(ret);
    }

}
