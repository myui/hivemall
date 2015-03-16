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

@Description(name = "mf_predict", value = "_FUNC_(List<Float> Pu, List<Float> Qi[, double Bu, double Bi[, double mu]]) - Returns the prediction value")
@UDFType(deterministic = true, stateful = false)
public final class MFPredictionUDF extends UDF {

    public FloatWritable evaluate(List<Float> Pu, List<Float> Qi) throws HiveException {
        if(Pu == null || Qi == null) {
            return null; //throw new HiveException("Pu should not be NULL");
        }
        final int factor = Pu.size();
        if(Qi.size() != factor) {
            throw new HiveException("|Pu| " + factor + " was not equal to |Qi| " + Qi.size());
        }
        float ret = 0.f;
        for(int k = 0; k < factor; k++) {
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
        if(Pu == null && Qi == null) {
            return null; //throw new HiveException("Both Pu and Qi was NULL");
        }

        if(Pu == null) {// TODO REVIEWME
            float ret = (float) (mu + Bi);
            return new FloatWritable(ret);
        } else if(Qi == null) {
            float ret = (float) (mu + Bu);
            return new FloatWritable(ret);
        }

        final int factor = Pu.size();
        if(Qi.size() != factor) {
            throw new HiveException("|Pu| " + factor + " was not equal to |Qi| " + Qi.size());
        }
        float ret = (float) (mu + Bu + Bi);
        for(int k = 0; k < factor; k++) {
            ret += Pu.get(k) * Qi.get(k);
        }
        return new FloatWritable(ret);
    }

}
