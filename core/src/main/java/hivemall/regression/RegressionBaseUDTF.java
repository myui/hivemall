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
package hivemall.regression;

import hivemall.LearnerBaseUDTF;
import hivemall.model.FeatureValue;
import hivemall.model.IWeightValue;
import hivemall.model.PredictionModel;
import hivemall.model.PredictionResult;
import hivemall.model.WeightValue;
import hivemall.model.WeightValue.WeightValueWithCovar;
import hivemall.optimizer.Optimizer;
import hivemall.utils.collections.IMapIterator;
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.lang.FloatAccumulator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.FloatWritable;

/**
 * The base class for regression algorithms. RegressionBaseUDTF provides general implementation for
 * online training and batch training.
 */
public abstract class RegressionBaseUDTF extends LearnerBaseUDTF {
    private static final Log logger = LogFactory.getLog(RegressionBaseUDTF.class);

    private ListObjectInspector featureListOI;
    private PrimitiveObjectInspector featureInputOI;
    private PrimitiveObjectInspector targetOI;
    private boolean parseFeature;

    protected PredictionModel model;
    protected Optimizer optimizerImpl;
    protected int count;

    // The accumulated delta of each weight values.
    protected transient Map<Object, FloatAccumulator> accumulated;
    protected int sampled;

    private boolean enableNewModel;

    public RegressionBaseUDTF() {
        this.enableNewModel = false;
    }

    public RegressionBaseUDTF(boolean enableNewModel) {
        this.enableNewModel = enableNewModel;
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if (argOIs.length < 2) {
            throw new UDFArgumentException(
                "_FUNC_ takes 2 arguments: List<Int|BigInt|Text> features, float target [, constant string options]");
        }
        this.featureInputOI = processFeaturesOI(argOIs[0]);
        this.targetOI = HiveUtils.asDoubleCompatibleOI(argOIs[1]);

        processOptions(argOIs);

        PrimitiveObjectInspector featureOutputOI = dense_model ? PrimitiveObjectInspectorFactory.javaIntObjectInspector
                : featureInputOI;
        this.model = enableNewModel? createNewModel(null) : createModel();
        if (preloadedModelFile != null) {
            loadPredictionModel(model, preloadedModelFile, featureOutputOI);
        }
        this.optimizerImpl = createOptimizer();

        this.count = 0;
        this.sampled = 0;
        return getReturnOI(featureOutputOI);
    }

    protected PrimitiveObjectInspector processFeaturesOI(ObjectInspector arg)
            throws UDFArgumentException {
        this.featureListOI = (ListObjectInspector) arg;
        ObjectInspector featureRawOI = featureListOI.getListElementObjectInspector();
        HiveUtils.validateFeatureOI(featureRawOI);
        this.parseFeature = HiveUtils.isStringOI(featureRawOI);
        return HiveUtils.asPrimitiveObjectInspector(featureRawOI);
    }

    protected StructObjectInspector getReturnOI(ObjectInspector featureOutputOI) {
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("feature");
        ObjectInspector featureOI = ObjectInspectorUtils.getStandardObjectInspector(featureOutputOI);
        fieldOIs.add(featureOI);
        fieldNames.add("weight");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableFloatObjectInspector);
        if (useCovariance()) {
            fieldNames.add("covar");
            fieldOIs.add(PrimitiveObjectInspectorFactory.writableFloatObjectInspector);
        }

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        if (is_mini_batch && accumulated == null) {
            this.accumulated = new HashMap<Object, FloatAccumulator>(1024);
        }

        List<?> features = (List<?>) featureListOI.getList(args[0]);
        FeatureValue[] featureVector = parseFeatures(features);
        if (featureVector == null) {
            return;
        }
        float target = PrimitiveObjectInspectorUtils.getFloat(args[1], targetOI);
        checkTargetValue(target);

        count++;

        train(featureVector, target);
    }

    @Nullable
    protected final FeatureValue[] parseFeatures(@Nonnull final List<?> features) {
        final int size = features.size();
        if (size == 0) {
            return null;
        }

        final ObjectInspector featureInspector = featureListOI.getListElementObjectInspector();
        final FeatureValue[] featureVector = new FeatureValue[size];
        for (int i = 0; i < size; i++) {
            Object f = features.get(i);
            if (f == null) {
                continue;
            }
            final FeatureValue fv;
            if (parseFeature) {
                fv = FeatureValue.parse(f);
            } else {
                Object k = ObjectInspectorUtils.copyToStandardObject(f, featureInspector);
                fv = new FeatureValue(k, 1.f);
            }
            featureVector[i] = fv;
        }
        return featureVector;
    }

    protected void checkTargetValue(float target) throws UDFArgumentException {}

    protected void train(@Nonnull final FeatureValue[] features, final float target) {
        float p = predict(features);
        update(features, target, p);
    }

    protected float predict(@Nonnull final FeatureValue[] features) {
        float score = 0.f;
        for (FeatureValue f : features) {// a += w[i] * x[i]
            if (f == null) {
                continue;
            }
            final Object k = f.getFeature();
            final float v = f.getValueAsFloat();

            float old_w = model.getWeight(k);
            if (old_w != 0f) {
                score += (old_w * v);
            }
        }
        return score;
    }

    @Nonnull
    protected PredictionResult calcScoreAndNorm(@Nonnull final FeatureValue[] features) {
        float score = 0.f;
        float squared_norm = 0.f;

        for (FeatureValue f : features) {// a += w[i] * x[i]
            if (f == null) {
                continue;
            }
            final Object k = f.getFeature();
            final float v = f.getValueAsFloat();

            float old_w = model.getWeight(k);
            if (old_w != 0f) {
                score += (old_w * v);
            }
            squared_norm += (v * v);
        }
        return new PredictionResult(score).squaredNorm(squared_norm);
    }

    @Nonnull
    protected PredictionResult calcScoreAndVariance(@Nonnull final FeatureValue[] features) {
        float score = 0.f;
        float variance = 0.f;

        for (FeatureValue f : features) {// a += w[i] * x[i]
            if (f == null) {
                continue;
            }
            final Object k = f.getFeature();
            final float v = f.getValueAsFloat();

            IWeightValue old_w = model.get(k);
            if (old_w == null) {
                variance += (v * v);
            } else {
                score += (old_w.get() * v);
                variance += (old_w.getCovariance() * v * v);
            }
        }

        return new PredictionResult(score).variance(variance);
    }

    protected void update(@Nonnull final FeatureValue[] features, final float target,
            final float predicted) {
        final float grad = computeGradient(target, predicted);

        if (is_mini_batch) {
            accumulateUpdate(features, grad);
            if (sampled >= mini_batch_size) {
                batchUpdate();
            }
        } else {
            onlineUpdate(features, grad);
        }
    }

    // Compute a gradient by using a loss function in derived classes
    protected float computeGradient(float target, float predicted) {
        throw new UnsupportedOperationException();
    }

    protected final void accumulateUpdate(@Nonnull final FeatureValue[] features, final float coeff) {
        for (int i = 0; i < features.length; i++) {
            if (features[i] == null) {
                continue;
            }
            final Object x = features[i].getFeature();
            final float xi = features[i].getValueAsFloat();
            float delta = xi * coeff;

            FloatAccumulator acc = accumulated.get(x);
            if (acc == null) {
                acc = new FloatAccumulator(delta);
                accumulated.put(x, acc);
            } else {
                acc.add(delta);
            }
        }
        sampled++;
    }

    protected final void batchUpdate() {
        if (accumulated.isEmpty()) {
            this.sampled = 0;
            return;
        }

        for (Map.Entry<Object, FloatAccumulator> e : accumulated.entrySet()) {
            Object x = e.getKey();
            FloatAccumulator v = e.getValue();
            float delta = v.get();

            float old_w = model.getWeight(x);
            float new_w = old_w + delta;
            model.set(x, new WeightValue(new_w));
        }
        accumulated.clear();
        this.sampled = 0;
    }

    /**
     * Calculate the update value for online training.
     */
    protected void onlineUpdate(@Nonnull final FeatureValue[] features, float coeff) {
        for (FeatureValue f : features) {// w[i] += y * x[i]
            if (f == null) {
                continue;
            }
            final Object x = f.getFeature();
            final float xi = f.getValueAsFloat();

            float old_w = model.getWeight(x);
            float new_w = old_w + (coeff * xi);
            model.set(x, new WeightValue(new_w));
        }
    }

    @Override
    public final void close() throws HiveException {
        super.close();
        if (model != null) {
            if (accumulated != null) { // Update model with accumulated delta
                batchUpdate();
                this.accumulated = null;
            }
            int numForwarded = 0;
            if (useCovariance()) {
                final WeightValueWithCovar probe = new WeightValueWithCovar();
                final Object[] forwardMapObj = new Object[3];
                final FloatWritable fv = new FloatWritable();
                final FloatWritable cov = new FloatWritable();
                final IMapIterator<Object, IWeightValue> itor = model.entries();
                while (itor.next() != -1) {
                    itor.getValue(probe);
                    if (!probe.isTouched()) {
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
                final IMapIterator<Object, IWeightValue> itor = model.entries();
                while (itor.next() != -1) {
                    itor.getValue(probe);
                    if (!probe.isTouched()) {
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
            long numMixed = model.getNumMixed();
            this.model = null;
            logger.info("Trained a prediction model using " + count + " training examples"
                    + (numMixed > 0 ? "( numMixed: " + numMixed + " )" : ""));
            logger.info("Forwarded the prediction model of " + numForwarded + " rows");
        }
    }

}
