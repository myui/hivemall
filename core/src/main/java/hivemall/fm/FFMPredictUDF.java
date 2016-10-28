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
package hivemall.fm;

import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.lang.NumberUtils;

import java.io.IOException;
import java.util.Arrays;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

@Description(name = "ffm_predict",
        value = "_FUNC_(string modelId, string model, array<string> features)"
                + " returns a prediction result in double from a Field-aware Factorization Machine")
@UDFType(deterministic = true, stateful = false)
public final class FFMPredictUDF extends GenericUDF {

    private StringObjectInspector _modelIdOI;
    private StringObjectInspector _modelOI;
    private ListObjectInspector _featureListOI;

    private DoubleWritable _result;
    @Nullable
    private String _cachedModeId;
    @Nullable
    private FFMPredictionModel _cachedModel;
    @Nullable
    private Feature[] _probes;

    public FFMPredictUDF() {}

    @Override
    public ObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if (argOIs.length != 3) {
            throw new UDFArgumentException("_FUNC_ takes 3 arguments");
        }
        this._modelIdOI = HiveUtils.asStringOI(argOIs[0]);
        this._modelOI = HiveUtils.asStringOI(argOIs[1]);
        this._featureListOI = HiveUtils.asListOI(argOIs[2]);

        this._result = new DoubleWritable();
        return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        String modelId = _modelIdOI.getPrimitiveJavaObject(args[0].get());
        if (modelId == null) {
            throw new HiveException("modelId is not set");
        }

        final FFMPredictionModel model;
        if (modelId.equals(_cachedModeId)) {
            model = this._cachedModel;
        } else {
            Text serModel = _modelOI.getPrimitiveWritableObject(args[1].get());
            if (serModel == null) {
                throw new HiveException("Model is null for model ID: " + modelId);
            }
            byte[] b = serModel.getBytes();
            final int length = serModel.getLength();
            try {
                model = FFMPredictionModel.deserialize(b, length);
                b = null;
            } catch (ClassNotFoundException e) {
                throw new HiveException(e);
            } catch (IOException e) {
                throw new HiveException(e);
            }
            this._cachedModeId = modelId;
            this._cachedModel = model;
        }

        int numFeatures = model.getNumFeatures();
        int numFields = model.getNumFields();

        Object arg2 = args[2].get();
        // [workaround]
        // java.lang.ClassCastException: org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray
        // cannot be cast to [Ljava.lang.Object;
        if (arg2 instanceof LazyBinaryArray) {
            arg2 = ((LazyBinaryArray) arg2).getList();
        }
        Feature[] x = Feature.parseFFMFeatures(arg2, _featureListOI, _probes, numFeatures,
            numFields);
        if (x == null || x.length == 0) {
            return null; // return NULL if there are no features
        }
        this._probes = x;

        double predicted = predict(x, model);
        _result.set(predicted);
        return _result;
    }

    private static double predict(@Nonnull final Feature[] x,
            @Nonnull final FFMPredictionModel model) throws HiveException {
        // w0
        double ret = model.getW0();
        // W
        for (Feature e : x) {
            double xi = e.getValue();
            float wi = model.getW(e);
            double wx = wi * xi;
            ret += wx;
        }
        // V        
        final int factors = model.getNumFactors();
        final float[] vij = new float[factors];
        final float[] vji = new float[factors];
        for (int i = 0; i < x.length; ++i) {
            final Feature ei = x[i];
            final double xi = ei.getValue();
            final int iField = ei.getField();
            for (int j = i + 1; j < x.length; ++j) {
                final Feature ej = x[j];
                final double xj = ej.getValue();
                final int jField = ej.getField();
                if (!model.getV(ei, jField, vij)) {
                    continue;
                }
                if (!model.getV(ej, iField, vij)) {
                    continue;
                }
                for (int f = 0; f < factors; f++) {
                    float vijf = vij[f];
                    float vjif = vji[f];
                    ret += vijf * vjif * xi * xj;
                }
            }
        }
        if (!NumberUtils.isFinite(ret)) {
            throw new HiveException("Detected " + ret + " in ffm_predict");
        }
        return ret;
    }

    @Override
    public void close() throws IOException {
        super.close();
        // clean up to help GC
        this._cachedModel = null;
        this._probes = null;
    }

    @Override
    public String getDisplayString(String[] args) {
        return "ffm_predict(" + Arrays.toString(args) + ")";
    }

}
