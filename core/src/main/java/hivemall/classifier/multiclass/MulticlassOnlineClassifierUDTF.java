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
package hivemall.classifier.multiclass;

import static hivemall.HivemallConstants.BIGINT_TYPE_NAME;
import static hivemall.HivemallConstants.INT_TYPE_NAME;
import static hivemall.HivemallConstants.STRING_TYPE_NAME;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
import hivemall.LearnerBaseUDTF;
import hivemall.io.FeatureValue;
import hivemall.io.IWeightValue;
import hivemall.io.Margin;
import hivemall.io.PredictionModel;
import hivemall.io.PredictionResult;
import hivemall.io.WeightValue;
import hivemall.io.WeightValue.WeightValueWithCovar;
import hivemall.utils.collections.IMapIterator;
import hivemall.utils.datetime.StopWatch;
import hivemall.utils.hadoop.HadoopUtils;
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.io.IOUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableFloatObjectInspector;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

public abstract class MulticlassOnlineClassifierUDTF extends LearnerBaseUDTF {
    private static final Log logger = LogFactory.getLog(MulticlassOnlineClassifierUDTF.class);

    private ListObjectInspector featureListOI;
    private boolean parseFeature;
    private PrimitiveObjectInspector labelInputOI;

    protected Map<Object, PredictionModel> label2model;
    protected int count;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if(argOIs.length < 2) {
            throw new UDFArgumentException(getClass().getSimpleName()
                    + " takes 2 arguments: List<Int|BigInt|Text> features, {Int|BitInt|Text} label [, constant text options]");
        }
        PrimitiveObjectInspector featureInputOI = processFeaturesOI(argOIs[0]);
        this.labelInputOI = HiveUtils.asPrimitiveObjectInspector(argOIs[1]);
        String labelTypeName = labelInputOI.getTypeName();
        if(!STRING_TYPE_NAME.equals(labelTypeName) && !INT_TYPE_NAME.equals(labelTypeName)
                && !BIGINT_TYPE_NAME.equals(labelTypeName)) {
            throw new UDFArgumentTypeException(0, "label must be a type [Int|BigInt|Text]: "
                    + labelTypeName);
        }

        processOptions(argOIs);

        PrimitiveObjectInspector featureOutputOI = dense_model
                ? PrimitiveObjectInspectorFactory.javaIntObjectInspector : featureInputOI;
        this.label2model = new HashMap<Object, PredictionModel>(64);
        if(preloadedModelFile != null) {
            loadPredictionModel(label2model, preloadedModelFile, labelInputOI, featureOutputOI);
        }

        this.count = 0;
        return getReturnOI(labelInputOI, featureOutputOI);
    }

    @Override
    protected int getInitialModelSize() {
        return 8192;
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

    protected StructObjectInspector getReturnOI(ObjectInspector labelRawOI, ObjectInspector featureRawOI) {
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
        if(useCovariance()) {
            fieldNames.add("covar");
            fieldOIs.add(PrimitiveObjectInspectorFactory.writableFloatObjectInspector);
        }

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        List<?> features = (List<?>) featureListOI.getList(args[0]);
        FeatureValue[] featureVector = parseFeatures(features);
        if(featureVector == null) {
            return;
        }
        Object label = ObjectInspectorUtils.copyToStandardObject(args[1], labelInputOI);
        if(label == null) {
            throw new UDFArgumentException("label value must not be NULL");
        }

        count++;
        train(featureVector, label);
    }

    @Nullable
    protected final FeatureValue[] parseFeatures(@Nonnull final List<?> features) {
        final int size = features.size();
        if(size == 0) {
            return null;
        }

        final ObjectInspector featureInspector = featureListOI.getListElementObjectInspector();
        final FeatureValue[] featureVector = new FeatureValue[size];
        for(int i = 0; i < size; i++) {
            Object f = features.get(i);
            if(f == null) {
                continue;
            }
            final FeatureValue fv;
            if(parseFeature) {
                fv = FeatureValue.parse(f);
            } else {
                Object k = ObjectInspectorUtils.copyToStandardObject(f, featureInspector);
                fv = new FeatureValue(k, 1.f);
            }
            featureVector[i] = fv;
        }
        return featureVector;
    }

    protected abstract void train(@Nonnull final FeatureValue[] features, @Nonnull final Object actual_label);

    protected final PredictionResult classify(@Nonnull final FeatureValue[] features) {
        float maxScore = Float.MIN_VALUE;
        Object maxScoredLabel = null;

        for(Map.Entry<Object, PredictionModel> label2map : label2model.entrySet()) {// for each class
            Object label = label2map.getKey();
            PredictionModel model = label2map.getValue();
            float score = calcScore(model, features);
            if(maxScoredLabel == null || score > maxScore) {
                maxScore = score;
                maxScoredLabel = label;
            }
        }

        return new PredictionResult(maxScoredLabel, maxScore);
    }

    protected Margin getMargin(@Nonnull final FeatureValue[] features, final Object actual_label) {
        float correctScore = 0.f;
        Object maxAnotherLabel = null;
        float maxAnotherScore = 0.f;

        for(Map.Entry<Object, PredictionModel> label2map : label2model.entrySet()) {// for each class
            Object label = label2map.getKey();
            PredictionModel model = label2map.getValue();
            float score = calcScore(model, features);
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

    protected Margin getMarginAndVariance(@Nonnull final FeatureValue[] features, final Object actual_label) {
        return getMarginAndVariance(features, actual_label, false);
    }

    protected Margin getMarginAndVariance(@Nonnull final FeatureValue[] features, final Object actual_label, boolean nonZeroVariance) {
        float correctScore = 0.f;
        float correctVariance = 0.f;
        Object maxAnotherLabel = null;
        float maxAnotherScore = 0.f;
        float maxAnotherVariance = 0.f;

        if(nonZeroVariance && label2model.isEmpty()) {// for initial call
            float var = 2.f * calcVariance(features);
            return new Margin(correctScore, maxAnotherLabel, maxAnotherScore).variance(var);
        }

        for(Map.Entry<Object, PredictionModel> label2map : label2model.entrySet()) {// for each class
            Object label = label2map.getKey();
            PredictionModel model = label2map.getValue();
            PredictionResult predicted = calcScoreAndVariance(model, features);
            float score = predicted.getScore();

            if(label.equals(actual_label)) {
                correctScore = score;
                correctVariance = predicted.getVariance();
            } else {
                if(maxAnotherLabel == null || score > maxAnotherScore) {
                    maxAnotherLabel = label;
                    maxAnotherScore = score;
                    maxAnotherVariance = predicted.getVariance();
                }
            }
        }

        float var = correctVariance + maxAnotherVariance;
        return new Margin(correctScore, maxAnotherLabel, maxAnotherScore).variance(var);
    }

    protected final float squaredNorm(@Nonnull final FeatureValue[] features) {
        float squared_norm = 0.f;
        for(FeatureValue f : features) {// a += w[i] * x[i]
            if(f == null) {
                continue;
            }
            final float v = f.getValue();
            squared_norm += (v * v);
        }
        return squared_norm;
    }

    protected final float calcScore(@Nonnull final PredictionModel model, @Nonnull final FeatureValue[] features) {
        float score = 0.f;
        for(FeatureValue f : features) {// a += w[i] * x[i]
            if(f == null) {
                continue;
            }
            final Object k = f.getFeature();
            final float v = f.getValue();

            float old_w = model.getWeight(k);
            if(old_w != 0f) {
                score += (old_w * v);
            }
        }
        return score;
    }

    protected final float calcVariance(@Nonnull final FeatureValue[] features) {
        float variance = 0.f;
        for(FeatureValue f : features) {// a += w[i] * x[i]
            if(f == null) {
                continue;
            }
            float v = f.getValue();
            variance += v * v;
        }
        return variance;
    }

    protected final PredictionResult calcScoreAndVariance(@Nonnull final PredictionModel model, @Nonnull final FeatureValue[] features) {
        float score = 0.f;
        float variance = 0.f;

        for(FeatureValue f : features) {// a += w[i] * x[i]
            if(f == null) {
                continue;
            }
            final Object k = f.getFeature();
            final float v = f.getValue();

            IWeightValue old_w = model.get(k);
            if(old_w == null) {
                variance += (1.f * v * v);
            } else {
                score += (old_w.get() * v);
                variance += (old_w.getCovariance() * v * v);
            }
        }

        return new PredictionResult(score).variance(variance);
    }

    protected void update(@Nonnull final FeatureValue[] features, float coeff, Object actual_label, Object missed_label) {
        assert (actual_label != null);
        if(actual_label.equals(missed_label)) {
            throw new IllegalArgumentException("Actual label equals to missed label: "
                    + actual_label);
        }

        PredictionModel model2add = label2model.get(actual_label);
        if(model2add == null) {
            model2add = createModel();
            label2model.put(actual_label, model2add);
        }
        PredictionModel model2sub = null;
        if(missed_label != null) {
            model2sub = label2model.get(missed_label);
            if(model2sub == null) {
                model2sub = createModel();
                label2model.put(missed_label, model2sub);
            }
        }

        for(FeatureValue f : features) {// w[f] += y * x[f]
            if(f == null) {
                continue;
            }
            final Object k = f.getFeature();
            final float v = f.getValue();

            float old_trueclass_w = model2add.getWeight(k);
            float add_w = old_trueclass_w + (coeff * v);
            model2add.set(k, new WeightValue(add_w));

            if(model2sub != null) {
                float old_falseclass_w = model2sub.getWeight(k);
                float sub_w = old_falseclass_w - (coeff * v);
                model2sub.set(k, new WeightValue(sub_w));
            }
        }
    }

    @Override
    public final void close() throws HiveException {
        super.close();
        if(label2model != null) {
            long numForwarded = 0L;
            long numMixed = 0L;
            if(useCovariance()) {
                final WeightValueWithCovar probe = new WeightValueWithCovar();
                final Object[] forwardMapObj = new Object[4];
                final FloatWritable fv = new FloatWritable();
                final FloatWritable cov = new FloatWritable();
                for(Map.Entry<Object, PredictionModel> entry : label2model.entrySet()) {
                    Object label = entry.getKey();
                    forwardMapObj[0] = label;
                    PredictionModel model = entry.getValue();
                    numMixed += model.getNumMixed();
                    IMapIterator<Object, IWeightValue> itor = model.entries();
                    while(itor.next() != -1) {
                        itor.getValue(probe);
                        if(!probe.isTouched()) {
                            continue; // skip outputting untouched weights
                        }
                        Object k = itor.getKey();
                        fv.set(probe.get());
                        cov.set(probe.getCovariance());
                        forwardMapObj[1] = k;
                        forwardMapObj[2] = fv;
                        forwardMapObj[3] = cov;
                        forward(forwardMapObj);
                        numForwarded++;
                    }
                }
            } else {
                final WeightValue probe = new WeightValue();
                final Object[] forwardMapObj = new Object[3];
                final FloatWritable fv = new FloatWritable();
                for(Map.Entry<Object, PredictionModel> entry : label2model.entrySet()) {
                    Object label = entry.getKey();
                    forwardMapObj[0] = label;
                    PredictionModel model = entry.getValue();
                    numMixed += model.getNumMixed();
                    IMapIterator<Object, IWeightValue> itor = model.entries();
                    while(itor.next() != -1) {
                        itor.getValue(probe);
                        if(!probe.isTouched()) {
                            continue; // skip outputting untouched weights
                        }
                        Object k = itor.getKey();
                        fv.set(probe.get());
                        forwardMapObj[1] = k;
                        forwardMapObj[2] = fv;
                        forward(forwardMapObj);
                        numForwarded++;
                    }
                }
            }
            this.label2model = null;
            logger.info("Trained a prediction model using " + count + " training examples"
                    + (numMixed > 0 ? "( numMixed: " + numMixed + " )" : ""));
            logger.info("Forwarded the prediction model of " + numForwarded + " rows");
        }
    }

    protected void loadPredictionModel(Map<Object, PredictionModel> label2model, String filename, PrimitiveObjectInspector labelOI, PrimitiveObjectInspector featureOI) {
        final StopWatch elapsed = new StopWatch();
        final long lines;
        try {
            if(useCovariance()) {
                lines = loadPredictionModel(label2model, new File(filename), labelOI, featureOI, writableFloatObjectInspector, writableFloatObjectInspector);
            } else {
                lines = loadPredictionModel(label2model, new File(filename), labelOI, featureOI, writableFloatObjectInspector);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load a model: " + filename, e);
        } catch (SerDeException e) {
            throw new RuntimeException("Failed to load a model: " + filename, e);
        }
        if(!label2model.isEmpty()) {
            long totalFeatures = 0L;
            StringBuilder statsBuf = new StringBuilder(256);
            for(Map.Entry<Object, PredictionModel> e : label2model.entrySet()) {
                Object label = e.getKey();
                int numFeatures = e.getValue().size();
                statsBuf.append('\n').append("Label: ").append(label).append(", Number of Features: ").append(numFeatures);
                totalFeatures += numFeatures;
            }
            logger.info("Loaded total " + totalFeatures + " features from distributed cache '"
                    + filename + "' (" + lines + " lines) in " + elapsed + statsBuf);
        }
    }

    private long loadPredictionModel(Map<Object, PredictionModel> label2model, File file, PrimitiveObjectInspector labelOI, PrimitiveObjectInspector featureOI, WritableFloatObjectInspector weightOI)
            throws IOException, SerDeException {
        long count = 0L;
        if(!file.exists()) {
            return count;
        }
        if(!file.getName().endsWith(".crc")) {
            if(file.isDirectory()) {
                for(File f : file.listFiles()) {
                    count += loadPredictionModel(label2model, f, labelOI, featureOI, weightOI);
                }
            } else {
                LazySimpleSerDe serde = HiveUtils.getLineSerde(labelOI, featureOI, weightOI);
                StructObjectInspector lineOI = (StructObjectInspector) serde.getObjectInspector();
                StructField c1ref = lineOI.getStructFieldRef("c1");
                StructField c2ref = lineOI.getStructFieldRef("c2");
                StructField c3ref = lineOI.getStructFieldRef("c3");
                PrimitiveObjectInspector c1refOI = (PrimitiveObjectInspector) c1ref.getFieldObjectInspector();
                PrimitiveObjectInspector c2refOI = (PrimitiveObjectInspector) c2ref.getFieldObjectInspector();
                FloatObjectInspector c3refOI = (FloatObjectInspector) c3ref.getFieldObjectInspector();

                BufferedReader reader = null;
                try {
                    reader = HadoopUtils.getBufferedReader(file);
                    String line;
                    while((line = reader.readLine()) != null) {
                        count++;
                        Text lineText = new Text(line);
                        Object lineObj = serde.deserialize(lineText);
                        List<Object> fields = lineOI.getStructFieldsDataAsList(lineObj);
                        Object f0 = fields.get(0);
                        Object f1 = fields.get(1);
                        Object f2 = fields.get(2);
                        if(f0 == null || f1 == null || f2 == null) {
                            continue; // avoid the case that key or value is null
                        }
                        Object label = c1refOI.getPrimitiveWritableObject(c1refOI.copyObject(f0));
                        PredictionModel model = label2model.get(label);
                        if(model == null) {
                            model = createModel();
                            label2model.put(label, model);
                        }
                        Object k = c2refOI.getPrimitiveWritableObject(c2refOI.copyObject(f1));
                        float v = c3refOI.get(f2);
                        model.set(k, new WeightValue(v, false));
                    }
                } finally {
                    IOUtils.closeQuietly(reader);
                }
            }
        }
        return count;
    }

    private long loadPredictionModel(Map<Object, PredictionModel> label2model, File file, PrimitiveObjectInspector labelOI, PrimitiveObjectInspector featureOI, WritableFloatObjectInspector weightOI, WritableFloatObjectInspector covarOI)
            throws IOException, SerDeException {
        long count = 0L;
        if(!file.exists()) {
            return count;
        }
        if(!file.getName().endsWith(".crc")) {
            if(file.isDirectory()) {
                for(File f : file.listFiles()) {
                    count += loadPredictionModel(label2model, f, labelOI, featureOI, weightOI, covarOI);
                }
            } else {
                LazySimpleSerDe serde = HiveUtils.getLineSerde(labelOI, featureOI, weightOI, covarOI);
                StructObjectInspector lineOI = (StructObjectInspector) serde.getObjectInspector();
                StructField c1ref = lineOI.getStructFieldRef("c1");
                StructField c2ref = lineOI.getStructFieldRef("c2");
                StructField c3ref = lineOI.getStructFieldRef("c3");
                StructField c4ref = lineOI.getStructFieldRef("c4");
                PrimitiveObjectInspector c1refOI = (PrimitiveObjectInspector) c1ref.getFieldObjectInspector();
                PrimitiveObjectInspector c2refOI = (PrimitiveObjectInspector) c2ref.getFieldObjectInspector();
                FloatObjectInspector c3refOI = (FloatObjectInspector) c3ref.getFieldObjectInspector();
                FloatObjectInspector c4refOI = (FloatObjectInspector) c4ref.getFieldObjectInspector();

                BufferedReader reader = null;
                try {
                    reader = HadoopUtils.getBufferedReader(file);
                    String line;
                    while((line = reader.readLine()) != null) {
                        count++;
                        Text lineText = new Text(line);
                        Object lineObj = serde.deserialize(lineText);
                        List<Object> fields = lineOI.getStructFieldsDataAsList(lineObj);
                        Object f0 = fields.get(0);
                        Object f1 = fields.get(1);
                        Object f2 = fields.get(2);
                        Object f3 = fields.get(3);
                        if(f0 == null || f1 == null || f2 == null) {
                            continue; // avoid unexpected case
                        }
                        Object label = c1refOI.getPrimitiveWritableObject(c1refOI.copyObject(f0));
                        PredictionModel model = label2model.get(label);
                        if(model == null) {
                            model = createModel();
                            label2model.put(label, model);
                        }
                        Object k = c2refOI.getPrimitiveWritableObject(c2refOI.copyObject(f1));
                        float v = c3refOI.get(f2);
                        float cov = (f3 == null) ? WeightValueWithCovar.DEFAULT_COVAR
                                : c4refOI.get(f3);
                        model.set(k, new WeightValueWithCovar(v, cov, false));
                    }
                } finally {
                    IOUtils.closeQuietly(reader);
                }
            }
        }
        return count;
    }
}
