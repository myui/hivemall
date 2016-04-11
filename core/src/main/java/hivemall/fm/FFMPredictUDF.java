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

import hivemall.utils.lang.NumberUtils;

import java.util.Arrays;

import javax.annotation.Nonnull;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

@Description(
        name = "ffm_predict",
        value = "_FUNC_(array[string] x, string model) returns a prediction result of Feild-aware Factorization Machines")
@UDFType(deterministic = true, stateful = false)
public final class FFMPredictUDF extends GenericUDF {

    private FFMPredictionModel model;

    public FFMPredictUDF() {}

    @Override
    public ObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        // FIXME
        model = null;
        return null;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        // FIXME
        Feature[] x = null;
        double result = predict(x, model);
        return new DoubleWritable(result);
    }

    @Override
    public String getDisplayString(String[] args) {
        return "ffm_predict(" + Arrays.toString(args) + ")";
    }

    private static double predict(@Nonnull Feature[] x, @Nonnull FFMPredictionModel model)
            throws HiveException {
        // w0
        double ret = model.getW0();
        // W
        for (Feature e : x) {
            double xj = e.getValue();
            float w = model.getW(e);
            double wx = w * xj;
            ret += wx;
        }
        // V
        for (int i = 0; i < x.length; ++i) {
            for (int f = 0, k = model.getNumFactors(); f < k; f++) {
                Feature ei = x[i];
                double xi = ei.getValue();
                for (int j = i + 1; j < x.length; ++j) {
                    Feature ej = x[j];
                    double xj = ej.getValue();
                    float vijf = model.getV(ei, ej.getField(), f);
                    float vjif = model.getV(ej, ei.getField(), f);
                    ret += vijf * vjif * xi * xj;
                    assert (!Double.isNaN(ret));
                }
            }
        }
        if (!NumberUtils.isFinite(ret)) {
            throw new HiveException("Detected " + ret + " in ffm_predict");
        }
        return ret;
    }

}
