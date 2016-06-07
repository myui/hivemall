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
package hivemall.mf;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.FloatWritable;

@Description(
        name = "mf_predict",
        value = "_FUNC_(List<Float> Pu, List<Float> Qi[, double Bu, double Bi[, double mu]]) - Returns the prediction value")
@UDFType(deterministic = true, stateful = false)
public final class MFPredictionUDF extends UDF {

    public FloatWritable evaluate(List<Float> Pu, List<Float> Qi) throws HiveException {
        return evaluate(Pu, Qi, 0.d);
    }

    public FloatWritable evaluate(List<Float> Pu, List<Float> Qi, double mu) throws HiveException {
        if (Pu == null || Qi == null) {
            return new FloatWritable((float) mu);
        }

        final int PuSize = Pu.size();
        final int QiSize = Qi.size();
        // workaround for TD
        if (PuSize == 0) {
            return new FloatWritable((float) mu);
        } else if (QiSize == 0) {
            return new FloatWritable((float) mu);
        }

        if (QiSize != PuSize) {
            throw new HiveException("|Pu| " + PuSize + " was not equal to |Qi| " + QiSize);
        }

        float ret = (float) mu;
        for (int k = 0; k < PuSize; k++) {
            ret += Pu.get(k) * Qi.get(k);
        }
        return new FloatWritable(ret);
    }

    public FloatWritable evaluate(List<Float> Pu, List<Float> Qi, double Bu, double Bi)
            throws HiveException {
        return evaluate(Pu, Qi, Bu, Bi, 0.d);
    }

    public FloatWritable evaluate(List<Float> Pu, List<Float> Qi, double Bu, double Bi, double mu)
            throws HiveException {
        if (Pu == null && Qi == null) {
            return new FloatWritable((float) mu);
        }
        if (Pu == null) {
            float ret = (float) (mu + Bi);
            return new FloatWritable(ret);
        } else if (Qi == null) {
            float ret = (float) (mu + Bu);
            return new FloatWritable(ret);
        }

        final int PuSize = Pu.size();
        final int QiSize = Qi.size();
        // workaround for TD        
        if (PuSize == 0) {
            if (QiSize == 0) {
                return new FloatWritable((float) mu);
            } else {
                float ret = (float) (mu + Bi);
                return new FloatWritable(ret);
            }
        } else if (QiSize == 0) {
            float ret = (float) (mu + Bu);
            return new FloatWritable(ret);
        }

        if (QiSize != PuSize) {
            throw new HiveException("|Pu| " + PuSize + " was not equal to |Qi| " + QiSize);
        }

        float ret = (float) (mu + Bu + Bi);
        for (int k = 0; k < PuSize; k++) {
            ret += Pu.get(k) * Qi.get(k);
        }
        return new FloatWritable(ret);
    }

}
