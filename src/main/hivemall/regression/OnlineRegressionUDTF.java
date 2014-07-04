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
package hivemall.regression;

import static hivemall.HivemallConstants.BIAS_CLAUSE;
import static hivemall.HivemallConstants.BIAS_CLAUSE_INT;
import static hivemall.HivemallConstants.BIGINT_TYPE_NAME;
import static hivemall.HivemallConstants.INT_TYPE_NAME;
import static hivemall.HivemallConstants.STRING_TYPE_NAME;
import hivemall.LearnerBaseUDTF;
import hivemall.common.FeatureValue;
import hivemall.common.PredictionResult;
import hivemall.common.WeightValue;
import hivemall.common.WeightValue.WeightValueWithCovar;
import hivemall.utils.collections.OpenHashMap;
import hivemall.utils.collections.OpenHashMap.IMapIterator;
import hivemall.utils.hadoop.HiveUtils;

import java.util.ArrayList;
import java.util.Collection;
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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

public abstract class OnlineRegressionUDTF extends LearnerBaseUDTF {

    private static final Log logger = LogFactory.getLog(OnlineRegressionUDTF.class);

    protected ListObjectInspector featureListOI;
    protected PrimitiveObjectInspector featureInputOI;
    protected FloatObjectInspector targetOI;
    protected boolean parseX;
    protected Object biasKey;

    protected OpenHashMap<Object, WeightValue> weights;
    protected int count;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if(argOIs.length < 2) {
            throw new UDFArgumentException(getClass().getSimpleName()
                    + " takes 2 arguments: List<Int|BigInt|Text> features, float target [, constant string options]");
        }
        this.featureInputOI = processFeaturesOI(argOIs[0]);
        this.targetOI = (FloatObjectInspector) argOIs[1];

        processOptions(argOIs);

        ObjectInspector featureOutputOI = featureInputOI;
        if(parseX && feature_hashing) {
            featureOutputOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        }

        if(bias != 0.f) {
            this.biasKey = INT_TYPE_NAME.equals(featureOutputOI.getTypeName()) ? BIAS_CLAUSE_INT
                    : new Text(BIAS_CLAUSE);
        } else {
            this.biasKey = null;
        }

        this.weights = new OpenHashMap<Object, WeightValue>(16384);
        if(preloadedModelFile != null) {
            loadPredictionModel(weights, preloadedModelFile, featureInputOI);
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
            throw new UDFArgumentTypeException(0, "1st argument must be List of key type [Int|BitInt|Text]: "
                    + keyTypeName);
        }
        this.parseX = STRING_TYPE_NAME.equals(keyTypeName);
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
        if(returnCovariance()) {
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
        float target = targetOI.get(args[1]);
        checkTargetValue(target);

        count++;
        train(features, target);
    }

    protected void checkTargetValue(float target) throws UDFArgumentException {}

    protected void train(final Collection<?> features, final float target) {
        float p = predict(features);
        update(features, target, p);
    }

    protected float predict(final Collection<?> features) {
        final ObjectInspector featureInspector = this.featureInputOI;
        final boolean parseX = this.parseX;

        float score = 0.f;
        for(Object f : features) {// a += w[i] * x[i]
            if(f == null) {
                continue;
            }
            final Object k;
            final float v;
            if(parseX) {
                FeatureValue fv = FeatureValue.parse(f, feature_hashing);
                k = fv.getFeature();
                v = fv.getValue();
            } else {
                k = ObjectInspectorUtils.copyToStandardObject(f, featureInspector);
                v = 1.f;
            }
            WeightValue old_w = weights.get(k);
            if(old_w != null) {
                score += (old_w.get() * v);
            }
        }

        if(biasKey != null) {
            WeightValue biasWeight = weights.get(biasKey);
            if(biasWeight != null) {
                score += (biasWeight.get() * bias);
            }
        }

        return score;
    }

    protected PredictionResult calcScoreAndNorm(Collection<?> features) {
        final ObjectInspector featureInspector = this.featureInputOI;
        final boolean parseX = this.parseX;

        float score = 0.f;
        float squared_norm = 0.f;

        for(Object f : features) {// a += w[i] * x[i]
            if(f == null) {
                continue;
            }
            final Object k;
            final float v;
            if(parseX) {
                FeatureValue fv = FeatureValue.parse(f, feature_hashing);
                k = fv.getFeature();
                v = fv.getValue();
            } else {
                k = ObjectInspectorUtils.copyToStandardObject(f, featureInspector);
                v = 1.f;
            }
            WeightValue old_w = weights.get(k);
            if(old_w != null) {
                score += (old_w.get() * v);
            }
            squared_norm += (v * v);
        }

        if(biasKey != null) {
            WeightValue biasWeight = weights.get(biasKey);
            if(biasWeight != null) {
                score += (biasWeight.get() * bias);
            }
            squared_norm += (bias * bias); // REVIEWME
        }

        return new PredictionResult(score).squaredNorm(squared_norm);
    }

    protected PredictionResult calcScoreAndVariance(Collection<?> features) {
        final ObjectInspector featureInspector = featureListOI.getListElementObjectInspector();
        final boolean parseX = this.parseX;

        float score = 0.f;
        float variance = 0.f;

        for(Object f : features) {// a += w[i] * x[i]
            if(f == null) {
                continue;
            }
            final Object k;
            final float v;
            if(parseX) {
                FeatureValue fv = FeatureValue.parse(f, feature_hashing);
                k = fv.getFeature();
                v = fv.getValue();
            } else {
                k = ObjectInspectorUtils.copyToStandardObject(f, featureInspector);
                v = 1.f;
            }
            WeightValue old_w = weights.get(k);
            if(old_w == null) {
                variance += (1.f * v * v);
            } else {
                score += (old_w.getValue() * v);
                variance += (old_w.getCovariance() * v * v);
            }
        }

        if(biasKey != null) {
            WeightValue biasWeight = weights.get(biasKey);
            if(biasWeight == null) {
                variance += (1.f * bias * bias);
            } else {
                score += (biasWeight.getValue() * bias);
                variance += (biasWeight.getCovariance() * bias * bias);
            }
        }

        return new PredictionResult(score).variance(variance);
    }

    protected void update(Collection<?> features, float target, float predicted) {
        float d = dloss(target, predicted);
        update(features, d);
    }

    protected float dloss(float target, float predicted) {
        throw new IllegalStateException();
    }

    protected void update(Collection<?> features, float coeff) {
        final ObjectInspector featureInspector = this.featureInputOI;

        for(Object f : features) {// w[i] += y * x[i]
            if(f == null) {
                continue;
            }
            final Object x;
            final float xi;
            if(parseX) {
                FeatureValue fv = FeatureValue.parse(f, feature_hashing);
                x = fv.getFeature();
                xi = fv.getValue();
            } else {
                x = ObjectInspectorUtils.copyToStandardObject(f, featureInspector);
                xi = 1.f;
            }
            WeightValue old_w = weights.get(x);
            float new_w = (old_w == null) ? coeff * xi : old_w.get() + (coeff * xi);
            weights.put(x, new WeightValue(new_w));
        }

        if(biasKey != null) {
            WeightValue old_bias = weights.get(biasKey);
            float new_bias = (old_bias == null) ? coeff * bias : old_bias.get() + (coeff * bias);
            weights.put(biasKey, new WeightValue(new_bias));
        }
    }

    @Override
    public void close() throws HiveException {
        if(weights != null) {
            int numForwarded = 0;
            if(returnCovariance()) {
                final Object[] forwardMapObj = new Object[3];
                IMapIterator<Object, WeightValue> itor = weights.entries();
                while(itor.next() != -1) {
                    Object k = itor.unsafeGetAndFreeKey();
                    WeightValueWithCovar v = (WeightValueWithCovar) itor.unsafeGetAndFreeValue();
                    if(skipUntouched && !v.isTouched()) {
                        continue; // skip outputting untouched weights
                    }
                    FloatWritable fv = new FloatWritable(v.get());
                    FloatWritable cov = new FloatWritable(v.getCovariance());
                    forwardMapObj[0] = k;
                    forwardMapObj[1] = fv;
                    forwardMapObj[2] = cov;
                    forward(forwardMapObj);
                    numForwarded++;
                }
            } else {
                final Object[] forwardMapObj = new Object[2];
                IMapIterator<Object, WeightValue> itor = weights.entries();
                while(itor.next() != -1) {
                    Object k = itor.unsafeGetAndFreeKey();
                    WeightValue v = itor.unsafeGetAndFreeValue();
                    if(skipUntouched && !v.isTouched()) {
                        continue; // skip outputting untouched weights
                    }
                    FloatWritable fv = new FloatWritable(v.get());
                    forwardMapObj[0] = k;
                    forwardMapObj[1] = fv;
                    forward(forwardMapObj);
                    numForwarded++;
                }
            }
            this.weights = null;
            logger.info("Trained a prediction model using " + count
                    + " training examples. Forwarded the prediction model of " + numForwarded
                    + " rows");
        }
    }

}
