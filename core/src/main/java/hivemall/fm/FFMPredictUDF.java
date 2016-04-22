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

import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.lang.NumberUtils;

import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

@Description(
        name = "ffm_predict",
        value = "_FUNC_(array[string] X, string model) returns a prediction result from a Field-aware Factorization Machine")
@UDFType(deterministic = true, stateful = false)
public final class FFMPredictUDF extends GenericUDF {

    private ListObjectInspector xOI;
    private StringObjectInspector modelOI;

    private FFMPredictionModel model;

    public FFMPredictUDF() {}

    @Override
    public ObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if (argOIs.length != 2) {
            throw new UDFArgumentException("_FUNC_ takes 2 arguments");
        }

        xOI = HiveUtils.asListOI(argOIs[0]);
        if (!(xOI.getListElementObjectInspector() instanceof FloatObjectInspector)) {
            throw new UDFArgumentException("Elements of first argument must be float");
        }

        modelOI = HiveUtils.asStringOI(argOIs[1]);

        return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        @SuppressWarnings("unchecked")
        List<Feature> xList = (List<Feature>) xOI.getList(args[0]);
        Feature[] x = (Feature[]) xList.toArray();

        String serModel = modelOI.getPrimitiveWritableObject(args[1]);//FIXME
        model = new FFMPredictionModel(serModel);//FIXME

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
            float w = model.getW1(e);
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
