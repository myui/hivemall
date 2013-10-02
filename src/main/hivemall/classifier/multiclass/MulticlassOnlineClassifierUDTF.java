/**
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
package hivemall.classifier.multiclass;

import hivemall.common.FeatureValue;
import hivemall.common.HivemallConstants;
import hivemall.common.Margin;
import hivemall.common.PredictionResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantStringObjectInspector;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

public abstract class MulticlassOnlineClassifierUDTF extends GenericUDTF {

    protected ListObjectInspector featureListOI;
    protected boolean parseX;
    protected ObjectInspector labelRawOI;

    protected boolean feature_hashing;
    protected float bias;
    protected Object biasKey;

    protected Map<Object, Map<Object, FloatWritable>> label2FeatureWeight;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if(argOIs.length < 2) {
            throw new UDFArgumentException(getClass().getSimpleName()
                    + " takes 2 arguments: List<Int|BigInt|Text> features, {Int|Text} label [, constant text options]");
        }
        this.featureListOI = (ListObjectInspector) argOIs[0];
        ObjectInspector featureRawOI = featureListOI.getListElementObjectInspector();
        String keyTypeName = featureRawOI.getTypeName();
        if(keyTypeName != HivemallConstants.STRING_TYPE_NAME
                && keyTypeName != HivemallConstants.INT_TYPE_NAME
                && keyTypeName != HivemallConstants.BIGINT_TYPE_NAME) {
            throw new UDFArgumentTypeException(0, "1st argument must be Map of key type [Int|BitInt|Text]: "
                    + keyTypeName);
        }
        this.parseX = (keyTypeName == HivemallConstants.STRING_TYPE_NAME);
        this.labelRawOI = argOIs[1];
        String labelTypeName = labelRawOI.getTypeName();
        if(labelTypeName != HivemallConstants.STRING_TYPE_NAME
                && labelTypeName != HivemallConstants.INT_TYPE_NAME) {
            throw new UDFArgumentTypeException(0, "label must be a type [Int|Text]: " + keyTypeName);
        }

        processOptions(argOIs);

        if(parseX && feature_hashing) {
            featureRawOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        }

        if(bias != 0.f) {
            this.biasKey = (featureRawOI.getTypeName() == HivemallConstants.INT_TYPE_NAME) ? HivemallConstants.BIAS_CLAUSE_INT
                    : new Text(HivemallConstants.BIAS_CLAUSE);
        } else {
            this.biasKey = null;
        }

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("label");
        ObjectInspector labelOI = ObjectInspectorUtils.getStandardObjectInspector(labelRawOI);
        fieldOIs.add(labelOI);
        fieldNames.add("feature");
        ObjectInspector featureOI = ObjectInspectorUtils.getStandardObjectInspector(featureRawOI);
        fieldOIs.add(featureOI);
        fieldNames.add("weight");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableFloatObjectInspector);

        this.label2FeatureWeight = new HashMap<Object, Map<Object, FloatWritable>>(64);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    protected Options getOptions() {
        Options opts = new Options();
        opts.addOption("fh", "fhash", false, "Enable feature hashing (only used when feature is TEXT type) [default: off]");
        opts.addOption("b", "bias", true, "Bias clause. Use 0.0 to disable. [default 1.0]");
        return opts;
    }

    protected final CommandLine parseOptions(String optionValue) throws UDFArgumentException {
        String[] args = optionValue.split("\\s+");

        Options opts = getOptions();

        BasicParser parser = new BasicParser();
        final CommandLine cl;
        try {
            cl = parser.parse(opts, args);
        } catch (ParseException e) {
            throw new UDFArgumentException(e);
        }
        return cl;
    }

    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        boolean fhashFlag = false;
        float bias = 0.f;

        CommandLine cl = null;
        if(argOIs.length >= 3) {
            String rawArgs = ((WritableConstantStringObjectInspector) argOIs[2]).getWritableConstantValue().toString();
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

    @Override
    public void process(Object[] args) throws HiveException {
        List<?> features = (List<?>) featureListOI.getList(args[0]);
        if(features.isEmpty()) {
            return;
        }
        Object label = ObjectInspectorUtils.copyToStandardObject(args[1], labelRawOI);
        if(label == null) {
            throw new UDFArgumentException("label value must not be NULL");
        }

        train(features, label);
    }

    protected abstract void train(final List<?> features, final Object actual_label);

    protected final PredictionResult classify(final List<?> features) {
        float maxScore = Float.MIN_VALUE;
        Object maxScoredLabel = null;

        for(Map.Entry<Object, Map<Object, FloatWritable>> label2map : label2FeatureWeight.entrySet()) {// for each class
            Object label = label2map.getKey();
            Map<Object, FloatWritable> weights = label2map.getValue();
            float score = calcScore(weights, features);
            if(maxScoredLabel == null || score > maxScore) {
                maxScore = score;
                maxScoredLabel = label;
            }
        }

        return new PredictionResult(maxScoredLabel, maxScore);
    }

    protected Margin getMargin(final List<?> features, final Object actual_label) {
        float correctScore = 0.f;
        Object maxAnotherLabel = null;
        float maxAnotherScore = 0.f;

        for(Map.Entry<Object, Map<Object, FloatWritable>> label2map : label2FeatureWeight.entrySet()) {// for each class
            Object label = label2map.getKey();
            Map<Object, FloatWritable> weights = label2map.getValue();
            float score = calcScore(weights, features);
            if(label.equals(actual_label)) {
                correctScore = score;
            } else {
                if(maxAnotherLabel == null || score > maxAnotherScore) {
                    maxAnotherLabel = label;
                    maxAnotherScore = score;
                }
            }
        }

        return new Margin(correctScore, maxAnotherLabel, maxAnotherScore);
    }

    protected final float squaredNorm(final List<?> features) {
        float squared_norm = 0.f;

        for(Object f : features) {// a += w[i] * x[i]
            final float v;
            if(parseX) {
                FeatureValue fv = FeatureValue.parse(f, feature_hashing);
                v = fv.getValue();
            } else {
                v = 1.f;
            }
            squared_norm += (v * v);
        }

        if(bias != 0.f) {
            squared_norm += (bias * bias); // REVIEWME
        }

        return squared_norm;
    }

    protected final float calcScore(final Map<Object, FloatWritable> weights, final List<?> features) {
        final ObjectInspector featureInspector = featureListOI.getListElementObjectInspector();
        final boolean parseX = this.parseX;

        float score = 0.f;

        for(Object f : features) {// a += w[i] * x[i]
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
            FloatWritable old_w = weights.get(k);
            if(old_w != null) {
                score += (old_w.get() * v);
            }
        }

        if(bias != 0.f) {
            FloatWritable biasWeight = weights.get(biasKey);
            if(biasWeight != null) {
                score += biasWeight.get();
            }
        }

        return score;
    }

    protected void update(List<?> features, float coeff, Object actual_label, Object missed_label) {
        assert (actual_label != null);
        if(actual_label.equals(missed_label)) {
            throw new IllegalArgumentException("Actual label equals to missed label: "
                    + actual_label);
        }

        Map<Object, FloatWritable> weightsToAdd = label2FeatureWeight.get(actual_label);
        if(weightsToAdd == null) {
            weightsToAdd = new HashMap<Object, FloatWritable>(8192);
            label2FeatureWeight.put(actual_label, weightsToAdd);
        }
        Map<Object, FloatWritable> weightsToSub = null;
        if(missed_label != null) {
            weightsToSub = label2FeatureWeight.get(missed_label);
            if(weightsToSub == null) {
                weightsToSub = new HashMap<Object, FloatWritable>(8192);
                label2FeatureWeight.put(missed_label, weightsToSub);
            }
        }

        final ObjectInspector featureInspector = featureListOI.getListElementObjectInspector();

        for(Object f : features) {// w[f] += y * x[f]
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
            FloatWritable old_trueclass_w = weightsToAdd.get(k);
            float add_w = (old_trueclass_w == null) ? coeff * v : old_trueclass_w.get()
                    + (coeff * v);
            weightsToAdd.put(k, new FloatWritable(add_w));

            if(weightsToSub != null) {
                FloatWritable old_falseclass_w = weightsToSub.get(k);
                float sub_w = (old_falseclass_w == null) ? -(coeff * v) : old_falseclass_w.get()
                        - (coeff * v);
                weightsToSub.put(k, new FloatWritable(sub_w));
            }
        }

        if(biasKey != null) {
            FloatWritable old_trueclass_bias = weightsToAdd.get(biasKey);
            float add_bias = (old_trueclass_bias == null) ? coeff * bias : old_trueclass_bias.get()
                    + (coeff * bias);
            weightsToAdd.put(biasKey, new FloatWritable(add_bias));

            if(weightsToSub != null) {
                FloatWritable old_falseclass_bias = weightsToSub.get(biasKey);
                float sub_bias = (old_falseclass_bias == null) ? -(coeff * bias)
                        : old_falseclass_bias.get() - (coeff * bias);
                weightsToSub.put(biasKey, new FloatWritable(sub_bias));
            }
        }
    }

    @Override
    public void close() throws HiveException {
        if(label2FeatureWeight != null) {
            final Object[] forwardMapObj = new Object[3];
            for(Map.Entry<Object, Map<Object, FloatWritable>> label2map : label2FeatureWeight.entrySet()) {
                Object label = label2map.getKey();
                forwardMapObj[0] = label;
                Map<Object, FloatWritable> fvmap = label2map.getValue();
                for(Map.Entry<Object, FloatWritable> fv : fvmap.entrySet()) {
                    Object k = fv.getKey();
                    FloatWritable v = fv.getValue();
                    forwardMapObj[1] = k;
                    forwardMapObj[2] = v;
                    forward(forwardMapObj);
                }
            }
            this.label2FeatureWeight = null;
        }
    }

}
