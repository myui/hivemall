/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013
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
package hivemall.classifier;

import static hivemall.HivemallConstants.BIGINT_TYPE_NAME;
import static hivemall.HivemallConstants.INT_TYPE_NAME;
import static hivemall.HivemallConstants.STRING_TYPE_NAME;
import hivemall.LearnerBaseUDTF;
import hivemall.io.FeatureValue;
import hivemall.io.PredictionModel;
import hivemall.io.PredictionResult;
import hivemall.io.WeightValue;
import hivemall.io.WeightValue.WeightValueWithCovar;
import hivemall.utils.collections.IMapIterator;
import hivemall.utils.hadoop.HiveUtils;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.FloatWritable;

public abstract class BinaryOnlineClassifierUDTF extends LearnerBaseUDTF {
    private static final Log logger = LogFactory.getLog(BinaryOnlineClassifierUDTF.class);

    protected ListObjectInspector featureListOI;
    protected IntObjectInspector labelOI;
    protected boolean parseFeature;

    protected PredictionModel model;
    protected int count;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if(argOIs.length < 2) {
            throw new UDFArgumentException(getClass().getSimpleName()
                    + " takes 2 arguments: List<Int|BigInt|Text> features, int label [, constant string options]");
        }
        PrimitiveObjectInspector featureInputOI = processFeaturesOI(argOIs[0]);
        this.labelOI = (IntObjectInspector) argOIs[1];

        processOptions(argOIs);

        PrimitiveObjectInspector featureOutputOI = dense_model ? PrimitiveObjectInspectorFactory.javaIntObjectInspector
                : featureInputOI;
        this.model = createModel();
        if(preloadedModelFile != null) {
            loadPredictionModel(model, preloadedModelFile, featureOutputOI);
        }

        this.count = 0;
        return getReturnOI(featureOutputOI);
    }

    protected PrimitiveObjectInspector processFeaturesOI(ObjectInspector arg)
            throws UDFArgumentException {
        this.featureListOI = (ListObjectInspector) arg;
        ObjectInspector featureRawOI = featureListOI.getListElementObjectInspector();
        String keyTypeName = featureRawOI.getTypeName();
        if(!STRING_TYPE_NAME.equals(keyTypeName) && !INT_TYPE_NAME.equals(keyTypeName)
                && !BIGINT_TYPE_NAME.equals(keyTypeName)) {
            throw new UDFArgumentTypeException(0, "1st argument must be Map of key type [Int|BitInt|Text]: "
                    + keyTypeName);
        }
        this.parseFeature = STRING_TYPE_NAME.equals(keyTypeName);
        return HiveUtils.asPrimitiveObjectInspector(featureRawOI);
    }

    protected StructObjectInspector getReturnOI(ObjectInspector featureRawOI) {
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("feature");
        ObjectInspector featureOI = ObjectInspectorUtils.getStandardObjectInspector(featureRawOI);
        fieldOIs.add(featureOI);
        fieldNames.add("weight");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableFloatObjectInspector);
        if(useCovariance()) {
            fieldNames.add("covar");
            fieldOIs.add(PrimitiveObjectInspectorFactory.writableFloatObjectInspector);
        }

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        List<?> features = (List<?>) featureListOI.getList(args[0]);
        if(features.isEmpty()) {
            return;
        }
        int label = (int) labelOI.get(args[1]);
        checkLabelValue(label);

        count++;
        train(features, label);
    }

    protected void checkLabelValue(int label) throws UDFArgumentException {
        assert (label == -1 || label == 0 || label == 1) : label;
    }

    protected void train(final List<?> features, final int label) {
        final float y = label > 0 ? 1f : -1f;

        final float p = predict(features);
        final float z = p * y;
        if(z <= 0.f) { // miss labeled
            update(features, y, p);
        }
    }

    protected float predict(final List<?> features) {
        final ObjectInspector featureInspector = featureListOI.getListElementObjectInspector();
        final boolean parseFeature = this.parseFeature;

        float score = 0f;
        for(Object f : features) {// a += w[i] * x[i]
            if(f == null) {
                continue;
            }
            final Object k;
            final float v;
            if(parseFeature) {
                FeatureValue fv = FeatureValue.parse(f);
                k = fv.getFeature();
                v = fv.getValue();
            } else {
                k = ObjectInspectorUtils.copyToStandardObject(f, featureInspector);
                v = 1f;
            }
            float old_w = model.getWeight(k);
            if(old_w != 0f) {
                score += (old_w * v);
            }
        }
        return score;
    }

    protected PredictionResult calcScoreAndNorm(List<?> features) {
        final ObjectInspector featureInspector = featureListOI.getListElementObjectInspector();
        final boolean parseX = this.parseFeature;

        float score = 0.f;
        float squared_norm = 0.f;

        for(Object f : features) {// a += w[i] * x[i]
            if(f == null) {
                continue;
            }
            final Object k;
            final float v;
            if(parseX) {
                FeatureValue fv = FeatureValue.parse(f);
                k = fv.getFeature();
                v = fv.getValue();
            } else {
                k = ObjectInspectorUtils.copyToStandardObject(f, featureInspector);
                v = 1.f;
            }
            float old_w = model.getWeight(k);
            if(old_w != 0f) {
                score += (old_w * v);
            }
            squared_norm += (v * v);
        }

        return new PredictionResult(score).squaredNorm(squared_norm);
    }

    protected PredictionResult calcScoreAndVariance(List<?> features) {
        final ObjectInspector featureInspector = featureListOI.getListElementObjectInspector();
        final boolean parseFeature = this.parseFeature;

        float score = 0.f;
        float variance = 0.f;

        for(Object f : features) {// a += w[i] * x[i]
            if(f == null) {
                continue;
            }
            final Object k;
            final float v;
            if(parseFeature) {
                FeatureValue fv = FeatureValue.parse(f);
                k = fv.getFeature();
                v = fv.getValue();
            } else {
                k = ObjectInspectorUtils.copyToStandardObject(f, featureInspector);
                v = 1.f;
            }
            WeightValue old_w = model.get(k);
            if(old_w == null) {
                variance += (1.f * v * v);
            } else {
                score += (old_w.get() * v);
                variance += (old_w.getCovariance() * v * v);
            }
        }

        return new PredictionResult(score).variance(variance);
    }

    protected void update(List<?> features, float y, float p) {
        throw new IllegalStateException();
    }

    protected void update(final List<?> features, final float coeff) {
        final ObjectInspector featureInspector = featureListOI.getListElementObjectInspector();

        for(Object f : features) {// w[f] += y * x[f]
            if(f == null) {
                continue;
            }
            final Object k;
            final float v;
            if(parseFeature) {
                FeatureValue fv = FeatureValue.parse(f);
                k = fv.getFeature();
                v = fv.getValue();
            } else {
                k = ObjectInspectorUtils.copyToStandardObject(f, featureInspector);
                v = 1.f;
            }
            float old_w = model.getWeight(k);
            float new_w = old_w + (coeff * v);
            model.set(k, new WeightValue(new_w));
        }
    }

    @Override
    public final void close() throws HiveException {
        super.close();
        if(model != null) {
            int numForwarded = 0;
            if(useCovariance()) {
                final WeightValueWithCovar probe = new WeightValueWithCovar();
                final Object[] forwardMapObj = new Object[3];
                final FloatWritable fv = new FloatWritable();
                final FloatWritable cov = new FloatWritable();
                final IMapIterator<Object, WeightValue> itor = model.entries();
                while(itor.next() != -1) {
                    itor.getValue(probe);
                    if(!probe.isTouched()) {
                        continue; // skip outputting untouched weights
                    }
                    Object k = itor.getKey();
                    fv.set(probe.get());
                    cov.set(probe.getCovariance());
                    forwardMapObj[0] = k;
                    forwardMapObj[1] = fv;
                    forwardMapObj[2] = cov;
                    forward(forwardMapObj);
                    numForwarded++;
                }
            } else {
                final WeightValue probe = new WeightValue();
                final Object[] forwardMapObj = new Object[2];
                final FloatWritable fv = new FloatWritable();
                final IMapIterator<Object, WeightValue> itor = model.entries();
                while(itor.next() != -1) {
                    itor.getValue(probe);
                    if(!probe.isTouched()) {
                        continue; // skip outputting untouched weights
                    }
                    Object k = itor.getKey();
                    fv.set(probe.get());
                    forwardMapObj[0] = k;
                    forwardMapObj[1] = fv;
                    forward(forwardMapObj);
                    numForwarded++;
                }
            }
            int numMixed = model.getNumMixed();
            this.model = null;
            logger.info("Trained a prediction model using " + count + " training examples"
                    + (numMixed > 0 ? "( numMixed: " + numMixed + " )" : ""));
            logger.info("Forwarded the prediction model of " + numForwarded + " rows");
        }
    }

}
