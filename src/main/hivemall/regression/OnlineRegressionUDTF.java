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

import hivemall.common.FeatureValue;
import hivemall.common.HivemallConstants;
import hivemall.common.PredictionResult;

import java.util.ArrayList;
import java.util.Collection;
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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantStringObjectInspector;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

public abstract class OnlineRegressionUDTF extends GenericUDTF {

    protected ListObjectInspector featureListOI;
    protected ObjectInspector featureInputOI;
    protected FloatObjectInspector targetOI;
    protected boolean parseX;

    protected boolean feature_hashing;
    protected float bias;
    protected Object biasKey;

    protected Map<Object, FloatWritable> weights;
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
            this.biasKey = (featureOutputOI.getTypeName() == HivemallConstants.INT_TYPE_NAME) ? HivemallConstants.BIAS_CLAUSE_INT
                    : new Text(HivemallConstants.BIAS_CLAUSE);
        } else {
            this.biasKey = null;
        }

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("feature");
        ObjectInspector featureOI = ObjectInspectorUtils.getStandardObjectInspector(featureOutputOI);
        fieldOIs.add(featureOI);
        fieldNames.add("weight");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableFloatObjectInspector);

        this.weights = new HashMap<Object, FloatWritable>(8192);
        this.count = 1;

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    protected ObjectInspector processFeaturesOI(ObjectInspector arg)
            throws UDFArgumentTypeException {
        this.featureListOI = (ListObjectInspector) arg;
        ObjectInspector featureRawOI = featureListOI.getListElementObjectInspector();
        String keyTypeName = featureRawOI.getTypeName();
        if(keyTypeName != HivemallConstants.STRING_TYPE_NAME
                && keyTypeName != HivemallConstants.INT_TYPE_NAME
                && keyTypeName != HivemallConstants.BIGINT_TYPE_NAME) {
            throw new UDFArgumentTypeException(0, "1st argument must be Map of key type [Int|BitInt|Text]: "
                    + keyTypeName);
        }
        this.parseX = (keyTypeName == HivemallConstants.STRING_TYPE_NAME);
        return featureRawOI;
    }

    protected Options getOptions() {
        Options opts = new Options();
        opts.addOption("fh", "fhash", false, "Enable feature hashing (only used when feature is TEXT type) [default: off]");
        opts.addOption("b", "bias", true, "Bias clause [default 1.0, 0.0 to disable]");
        return opts;
    }

    private final CommandLine parseOptions(String optionValue) throws UDFArgumentException {
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
        float biasValue = 0.f;

        CommandLine cl = null;
        if(argOIs.length >= 3) {
            String rawArgs = ((WritableConstantStringObjectInspector) argOIs[2]).getWritableConstantValue().toString();
            cl = parseOptions(rawArgs);

            if(cl.hasOption("fh")) {
                fhashFlag = true;
            }

            String biasStr = cl.getOptionValue("b");
            if(biasStr != null) {
                biasValue = Float.parseFloat(biasStr);
            }
        }

        this.feature_hashing = fhashFlag;
        this.bias = biasValue;
        return cl;
    }

    @Override
    public void process(Object[] args) throws HiveException {
        List<?> features = (List<?>) featureListOI.getList(args[0]);
        float target = targetOI.get(args[1]);
        checkTargetValue(target);

        train(weights, features, target);
        count++;
    }

    protected void checkTargetValue(float target) throws UDFArgumentException {}

    protected void train(final Map<Object, FloatWritable> weights, final Collection<?> features, final float target) {
        float p = predict(features);
        update(features, target, p);
    }

    protected float predict(final Collection<?> features) {
        final ObjectInspector featureInspector = this.featureInputOI;
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

        if(biasKey != null) {
            FloatWritable biasWeight = weights.get(biasKey);
            if(biasWeight != null) {
                score += biasWeight.get();
            }
        }

        return score;
    }

    protected PredictionResult calcScore(Collection<?> features) {
        final ObjectInspector featureInspector = this.featureInputOI;
        final boolean parseX = this.parseX;

        float score = 0.f;
        float squared_norm = 0.f;

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
            squared_norm += (v * v);
        }

        if(biasKey != null) {
            FloatWritable biasWeight = weights.get(biasKey);
            if(biasWeight != null) {
                score += (biasWeight.get() * bias);
            }
            squared_norm += (bias * bias); // REVIEWME
        }

        return new PredictionResult(score).squaredNorm(squared_norm);
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
            FloatWritable old_w = weights.get(x);
            float new_w = (old_w == null) ? coeff * xi : old_w.get() + (coeff * xi);
            weights.put(x, new FloatWritable(new_w));
        }

        if(biasKey != null) {
            FloatWritable old_bias = weights.get(biasKey);
            float new_bias = (old_bias == null) ? coeff * bias : old_bias.get() + (coeff * bias);
            weights.put(biasKey, new FloatWritable(new_bias));
        }
    }

    @Override
    public void close() throws HiveException {
        if(weights != null) {
            final Object[] forwardMapObj = new Object[2];
            for(Map.Entry<Object, FloatWritable> e : weights.entrySet()) {
                Object k = e.getKey();
                FloatWritable v = e.getValue();
                forwardMapObj[0] = k;
                forwardMapObj[1] = v;
                forward(forwardMapObj);
            }
            this.weights = null;
        }
    }

}
