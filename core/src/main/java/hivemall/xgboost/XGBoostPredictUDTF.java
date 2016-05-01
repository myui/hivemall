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
package hivemall.xgboost;

import java.io.ByteArrayInputStream;
import java.util.*;
import java.util.Map.Entry;
import java.util.List;

import ml.dmlc.xgboost4j.LabeledPoint;
import ml.dmlc.xgboost4j.java.Booster;
import ml.dmlc.xgboost4j.java.DMatrix;
import ml.dmlc.xgboost4j.java.XGBoost;
import ml.dmlc.xgboost4j.java.XGBoostError;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

import hivemall.UDTFWithOptions;
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.lang.Primitives;

public abstract class XGBoostPredictUDTF extends UDTFWithOptions {

    // For input parameters
    private PrimitiveObjectInspector rowIdOI;
    private ListObjectInspector featureListOI;
    private PrimitiveObjectInspector featureElemOI;
    private PrimitiveObjectInspector modelIdOI;
    private PrimitiveObjectInspector modelOI;

    // For input buffer
    private Map<String, Booster> mapToModel;
    private Map<String, List<LabeledPointWithRowId>> rowBuffer;

    private int batch_size;

    // Settings for the XGBoost native library
    static {
        NativeLibLoader.initXGBoost();
    }

    public XGBoostPredictUDTF() {}

    protected final class LabeledPointWithRowId {
        public String rowId;
        public LabeledPoint point;

        // Prevent other classes from instantiating this
        LabeledPointWithRowId() {}
    }

    private LabeledPointWithRowId createLabeledPoint(String rowId, LabeledPoint point) {
        final LabeledPointWithRowId p = new LabeledPointWithRowId();
        p.rowId = rowId;
        p.point = point;
        return p;
    }

    @Override
    protected Options getOptions() {
        Options opts = new Options();
        opts.addOption("batch_size", true, "Number of rows to predict together [default: 128]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        int _batch_size = 128;
        CommandLine cl = null;
        if(argOIs.length >= 5) {
            String rawArgs = HiveUtils.getConstString(argOIs[4]);
            cl = this.parseOptions(rawArgs);
            _batch_size = Primitives.parseInt(cl.getOptionValue("_batch_size"), _batch_size);
            if(_batch_size < 1) {
                throw new IllegalArgumentException(
                        "batch_size must be greater than 0: " + _batch_size);
            }
        }
        this.batch_size = _batch_size;
        return cl;
    }

    /** Override this to output predicted results depending on a taks type */
    abstract public StructObjectInspector getReturnOI();

    abstract public void forwardPredicted(
            final List<LabeledPointWithRowId> testData,
            final float[][] predicted) throws HiveException;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if(argOIs.length != 4 && argOIs.length != 5) {
            throw new UDFArgumentException(this.getClass().getSimpleName()
                    + " takes 4 or 5 arguments: string rowid, string[] features, string model_id,"
                    + " array<byte> pred_model [, string options]: " + argOIs.length);
        } else {
            this.processOptions(argOIs);
            this.rowIdOI = HiveUtils.asStringOI(argOIs[0]);
            final ListObjectInspector listOI = HiveUtils.asListOI(argOIs[1]);
            final ObjectInspector elemOI = listOI.getListElementObjectInspector();
            this.featureListOI = listOI;
            this.featureElemOI = HiveUtils.asStringOI(elemOI);
            this.modelIdOI = HiveUtils.asStringOI(argOIs[2]);
            this.modelOI = HiveUtils.asBinaryOI(argOIs[3]);
            this.mapToModel = new HashMap<String, Booster>();
            this.rowBuffer = new HashMap<String, List<LabeledPointWithRowId>>();
            return getReturnOI();
        }
    }

    private static DMatrix createDMatrix(final List<LabeledPointWithRowId> data) throws XGBoostError {
        final List<LabeledPoint> points = new ArrayList(data.size());
        for(LabeledPointWithRowId d : data) {
            points.add(d.point);
        }
        return new DMatrix(points.iterator(), "");
    }

    private static Booster initXgBooster(final byte[] input) throws HiveException {
        try {
            return XGBoost.loadModel(new ByteArrayInputStream(input));
        } catch (Exception e) {
            throw new HiveException(e.getMessage());
        }
    }

    private void predictAndFlush(final Booster model, final List<LabeledPointWithRowId> buf)
            throws HiveException {
        try {
            final DMatrix testData = createDMatrix(buf);
            final float[][] predicted = model.predict(testData);
            forwardPredicted(buf, predicted);
        } catch (Exception e) {
            throw new HiveException(e.getMessage());
        }
        buf.clear();
    }

    @Override
    public void process(Object[] args) throws HiveException {
        if(args[1] != null) {
            final String rowId = PrimitiveObjectInspectorUtils.getString(args[0], rowIdOI);
            final List<String> features = (List<String>) featureListOI.getList(args[1]);
            final String modelId = PrimitiveObjectInspectorUtils.getString(args[2], modelIdOI);
            if(!mapToModel.containsKey(modelId)) {
                final byte[] predModel = PrimitiveObjectInspectorUtils.getBinary(args[3], modelOI).getBytes();
                mapToModel.put(modelId, initXgBooster(predModel));
            }
            final LabeledPoint point = XGBoostUtils.parseFeatures(0.f, features);
            if(point != null) {
                if(!rowBuffer.containsKey(modelId)) {
                    rowBuffer.put(modelId, new ArrayList());
                }
                final List<LabeledPointWithRowId> buf = rowBuffer.get(modelId);
                buf.add(createLabeledPoint(rowId, point));
                if(buf.size() >= batch_size) {
                    predictAndFlush(mapToModel.get(modelId), buf);
                }
            }
        }
    }

    @Override
    public void close() throws HiveException {
        for(Entry<String, List<LabeledPointWithRowId>> e : rowBuffer.entrySet()) {
            predictAndFlush(mapToModel.get(e.getKey()), e.getValue());
        }
    }

}
