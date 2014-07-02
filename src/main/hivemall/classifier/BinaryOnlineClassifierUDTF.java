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
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

public abstract class BinaryOnlineClassifierUDTF extends LearnerBaseUDTF {

    protected ListObjectInspector featureListOI;
    protected IntObjectInspector labelOI;
    protected boolean parseX;

    protected boolean feature_hashing;
    protected float bias;
    protected Object biasKey;

    protected OpenHashMap<Object, WeightValue> weights;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if(argOIs.length < 2) {
            throw new UDFArgumentException(getClass().getSimpleName()
                    + " takes 2 arguments: List<Int|BigInt|Text> features, int label [, constant string options]");
        }
        this.featureListOI = (ListObjectInspector) argOIs[0];
        ObjectInspector featureRawOI = featureListOI.getListElementObjectInspector();
        String keyTypeName = featureRawOI.getTypeName();
        if(!STRING_TYPE_NAME.equals(keyTypeName) && !INT_TYPE_NAME.equals(keyTypeName)
                && !BIGINT_TYPE_NAME.equals(keyTypeName)) {
            throw new UDFArgumentTypeException(0, "1st argument must be Map of key type [Int|BitInt|Text]: "
                    + keyTypeName);
        }
        this.parseX = STRING_TYPE_NAME.equals(keyTypeName);
        this.labelOI = (IntObjectInspector) argOIs[1];

        processOptions(argOIs);

        if(parseX && feature_hashing) {
            featureRawOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        }

        if(bias != 0.f) {
            this.biasKey = INT_TYPE_NAME.equals(featureRawOI.getTypeName()) ? BIAS_CLAUSE_INT
                    : new Text(BIAS_CLAUSE);
        } else {
            this.biasKey = null;
        }

        this.weights = new OpenHashMap<Object, WeightValue>(16384);

        return getReturnOI(featureRawOI);
    }

    @Override
    protected Options getOptions() {
        Options opts = new Options();
        opts.addOption("fh", "fhash", false, "Enable feature hashing (only used when feature is TEXT type) [default: off]");
        opts.addOption("b", "bias", true, "Bias clause [default 0.0 (disable)]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        boolean fhashFlag = false;
        float bias = 0.f;

        CommandLine cl = null;
        if(argOIs.length >= 3) {
            String rawArgs = HiveUtils.getConstString(argOIs[2]);
            cl = parseOptions(rawArgs);

            if(cl.hasOption("fh")) {
                fhashFlag = true;
            }

            String biasStr = cl.getOptionValue("b");
            if(biasStr != null) {
                bias = Float.parseFloat(biasStr);
            }
        }

        this.feature_hashing = fhashFlag;
        this.bias = bias;
        return cl;
    }

    protected StructObjectInspector getReturnOI(ObjectInspector featureRawOI) {
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("feature");
        ObjectInspector featureOI = ObjectInspectorUtils.getStandardObjectInspector(featureRawOI);
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
        int label = (int) labelOI.get(args[1]);
        checkLabelValue(label);

        train(features, label);
    }

    protected void checkLabelValue(int label) throws UDFArgumentException {
        assert (label == -1 || label == 0 || label == 1) : label;
    }

    protected void train(final List<?> features, final int label) {
        final int y = label > 0 ? 1 : -1;

        final float p = predict(features);
        final float z = p * y;
        if(z <= 0.f) { // miss labeled
            update(features, y, p);
        }
    }

    protected float predict(final List<?> features) {
        final ObjectInspector featureInspector = featureListOI.getListElementObjectInspector();
        final boolean parseX = this.parseX;

        float score = 0f;
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
                v = 1f;
            }
            WeightValue old_w = weights.get(k);
            if(old_w != null) {
                score += (old_w.getValue() * v);
            }
        }

        if(biasKey != null) {
            WeightValue biasWeight = weights.get(biasKey);
            if(biasWeight != null) {
                score += (biasWeight.getValue() * bias);
            }
        }

        return score;
    }

    protected PredictionResult calcScoreAndNorm(List<?> features) {
        final ObjectInspector featureInspector = featureListOI.getListElementObjectInspector();
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
                score += (old_w.getValue() * v);
            }
            squared_norm += (v * v);
        }

        if(biasKey != null) {
            WeightValue biasWeight = weights.get(biasKey);
            if(biasWeight != null) {
                score += (biasWeight.getValue() * bias);
            }
            squared_norm += (bias * bias);
        }

        return new PredictionResult(score).squaredNorm(squared_norm);
    }

    protected PredictionResult calcScoreAndVariance(List<?> features) {
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

    protected void update(List<?> features, int y, float p) {
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
            if(parseX) {
                FeatureValue fv = FeatureValue.parse(f, feature_hashing);
                k = fv.getFeature();
                v = fv.getValue();
            } else {
                k = ObjectInspectorUtils.copyToStandardObject(f, featureInspector);
                v = 1.f;
            }
            WeightValue old_w = weights.get(k);
            float new_w = (old_w == null) ? coeff * v : old_w.getValue() + (coeff * v);
            weights.put(k, new WeightValue(new_w));
        }

        if(biasKey != null) {
            WeightValue old_bias = weights.get(biasKey);
            float new_bias = (old_bias == null) ? coeff * bias : old_bias.getValue()
                    + (coeff * bias);
            weights.put(biasKey, new WeightValue(new_bias));
        }
    }

    @Override
    public void close() throws HiveException {
        if(weights != null) {
            if(returnCovariance()) {
                final Object[] forwardMapObj = new Object[3];
                IMapIterator<Object, WeightValue> itor = weights.entries();
                while(itor.next() != -1) {
                    Object k = itor.unsafeGetAndFreeKey();
                    WeightValueWithCovar v = (WeightValueWithCovar) itor.unsafeGetAndFreeValue();
                    FloatWritable fv = new FloatWritable(v.get());
                    FloatWritable cov = new FloatWritable(v.getCovariance());
                    forwardMapObj[0] = k;
                    forwardMapObj[1] = fv;
                    forwardMapObj[2] = cov;
                    forward(forwardMapObj);
                }
            } else {
                final Object[] forwardMapObj = new Object[2];
                IMapIterator<Object, WeightValue> itor = weights.entries();
                while(itor.next() != -1) {
                    Object k = itor.unsafeGetAndFreeKey();
                    WeightValue v = itor.unsafeGetAndFreeValue();
                    FloatWritable fv = new FloatWritable(v.get());
                    forwardMapObj[0] = k;
                    forwardMapObj[1] = fv;
                    forward(forwardMapObj);
                }
            }
            this.weights = null;
        }
    }

}
