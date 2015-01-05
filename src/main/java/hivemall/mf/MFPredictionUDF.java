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
