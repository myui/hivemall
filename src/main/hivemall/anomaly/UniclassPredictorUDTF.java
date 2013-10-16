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
package hivemall.anomaly;

import hivemall.common.FeatureValue;
import hivemall.common.HivemallConstants;
import hivemall.common.PredictionResult;
import hivemall.common.WeightValue;

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

public abstract class UniclassPredictorUDTF extends GenericUDTF {

    protected ListObjectInspector featureListOI;
    protected boolean parseX;

    protected boolean feature_hashing;
    protected float bias;
    protected Object biasKey;
    protected Object radiusKey;

    protected Map<Object, WeightValue> weights;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if(argOIs.length < 1) {
            throw new UDFArgumentException(getClass().getSimpleName()
                    + " takes at least 1 argument: List<Int|BigInt|Text> features [, constant string options]");
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
        if(learnRadius()) {
            this.radiusKey = (featureRawOI.getTypeName() == HivemallConstants.INT_TYPE_NAME) ? HivemallConstants.RADIUS_CLAUSE_INT
                    : new Text(HivemallConstants.RADIUS_CLAUSE);
        } else {
            this.radiusKey = null;
        }

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("feature");
        ObjectInspector featureOI = ObjectInspectorUtils.getStandardObjectInspector(featureRawOI);
        fieldOIs.add(featureOI);
        fieldNames.add("weight");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableFloatObjectInspector);

        this.weights = new HashMap<Object, WeightValue>(8192);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    protected Options getOptions() {
        Options opts = new Options();
        opts.addOption("fh", "fhash", false, "Enable feature hashing (only used when feature is TEXT type) [default: off]");
        opts.addOption("b", "bias", true, "Bias clause [default 0.0 (disable)]");
        return opts;
    }

    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        boolean fhashFlag = false;
        float bias = 0.f;

        CommandLine cl = null;
        if(argOIs.length >= 2) {
            String rawArgs = ((WritableConstantStringObjectInspector) argOIs[1]).getWritableConstantValue().toString();
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

    protected boolean learnRadius() {
        return false;
    }

    @Override
    public void process(Object[] args) throws HiveException {
        List<?> features = (List<?>) featureListOI.getList(args[0]);

        train(features);
    }

    protected void train(final List<?> features) {
        Map<Object, WeightValue> w = predict(features);
        PredictionResult margin = calcMarginAndNorm(features, w);
        float loss = loss(margin);
        if(loss > 0.f) {
            update(features, loss, margin);
        }
    }

    protected Map<Object, WeightValue> predict(final List<?> features) {
        return weights;
    }

    /**
     * Margin = W_t - Y_t, L2Norm = ||W_t - Y_t||
     */
    protected PredictionResult calcMarginAndNorm(List<?> features, Map<Object, WeightValue> weight) {
        final ObjectInspector featureInspector = featureListOI.getListElementObjectInspector();
        final boolean parseX = this.parseX;

        float margin = 0.f;
        float sqnorm = 0.f;

        for(Object f : features) {
            final Object k;
            final float y;
            if(parseX) {
                FeatureValue fv = FeatureValue.parse(f, feature_hashing);
                k = fv.getFeature();
                y = fv.getValue();
            } else {
                k = ObjectInspectorUtils.copyToStandardObject(f, featureInspector);
                y = 1.f;
            }
            WeightValue old_w = weights.get(k);
            float w = (old_w == null) ? 0.f : old_w.get();
            float m = w - y;
            margin += m;
            sqnorm += (m * m);
        }

        if(biasKey != null) {
            WeightValue biasWeight = weights.get(biasKey);
            float w = (biasWeight == null) ? 0.f : biasWeight.get();
            float m = w - bias;
            margin += m;
            sqnorm += (m * m);
        }

        if(radiusKey != null) {
            WeightValue radiusWeight = weights.get(radiusKey);
            assert (radiusWeight != null);
            float w = radiusWeight.get();
            float m = w; // w - 0.f;
            margin += m;
            sqnorm += (m * m);
        }

        return new PredictionResult(margin).squaredNorm(sqnorm);
    }

    protected abstract float loss(PredictionResult margin);

    protected abstract void update(List<?> features, float loss, PredictionResult margin);

    protected void update(List<?> features, float eta) {
        if(eta == 0.f) {
            return; // no need to update
        }

        final ObjectInspector featureInspector = featureListOI.getListElementObjectInspector();

        for(Object f : features) {// w[f] += eta * (Yt - Wt) / ||Yt - Wt||
            final Object k;
            final float y;
            if(parseX) {
                FeatureValue fv = FeatureValue.parse(f, feature_hashing);
                k = fv.getFeature();
                y = fv.getValue();
            } else {
                k = ObjectInspectorUtils.copyToStandardObject(f, featureInspector);
                y = 1.f;
            }
            WeightValue old_w = weights.get(k);
            WeightValue new_w = getNewWeight(old_w, y, eta);
            weights.put(k, new_w);
        }

        if(biasKey != null) {
            WeightValue old_bias = weights.get(biasKey);
            WeightValue new_bias = getNewWeight(old_bias, bias, eta);
            weights.put(biasKey, new_bias);
        }

        if(radiusKey != null) {
            WeightValue oldRadiusWeight = weights.get(radiusKey);
            if(oldRadiusWeight == null) {
                throw new IllegalStateException("Initial radius value is not defined");
            }
            WeightValue newRadiusWeight = getNewWeight(oldRadiusWeight, 0.f, eta);
            float new_w = newRadiusWeight.get();
            if(new_w <= 0.f) {
                throw new IllegalStateException("w must be bounded in (0,B]: " + new_w);
            }
            weights.put(radiusKey, newRadiusWeight); // decrease over time, bounded in (0,B] 
        }
    }

    protected static WeightValue getNewWeight(final WeightValue old, final float y, final float eta) {
        float w = (old == null) ? 0.f : old.get();
        float diff = y - w;
        if(diff == 0.f) {// Float.compare(diff, 0.f) == 0
            return new WeightValue(w);
        }
        float tauv = (diff > 0) ? eta : -eta; //eta * (diff / (float) MathUtils.l2Norm(diff));
        float new_w = w + tauv;
        return new WeightValue(new_w);
    }

    @Override
    public void close() throws HiveException {
        if(weights != null) {
            final Object[] forwardMapObj = new Object[2];
            for(Map.Entry<Object, WeightValue> e : weights.entrySet()) {
                Object k = e.getKey();
                WeightValue v = e.getValue();
                FloatWritable fv = new FloatWritable(v.getValue());
                forwardMapObj[0] = k;
                forwardMapObj[1] = fv;
                forward(forwardMapObj);
            }
            this.weights = null;
        }
    }

}
