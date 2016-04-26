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
package hivemall.xgboost.regression;

import java.lang.reflect.Constructor;
import java.util.*;

import ml.dmlc.xgboost4j.LabeledPoint;
import ml.dmlc.xgboost4j.java.Booster;
import ml.dmlc.xgboost4j.java.DMatrix;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

import hivemall.UDTFWithOptions;
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.lang.Primitives;

@Description(
    name = "train_xgboost",
    value = "_FUNC_(string[] features, double target [, string options]) - Returns a relation consisting of <string model_id, array<byte> pred_model>"
)
public final class XGBoostUDTF extends UDTFWithOptions {
    private static final Log logger = LogFactory.getLog(XGBoostUDTF.class);

    private ListObjectInspector featureListOI;
    private PrimitiveObjectInspector featureElemOI;
    private PrimitiveObjectInspector targetOI;

    // Parameters used inside
    private Map<String, Object> xgboostParams;
    private int num_boosting_iter;

    private List<LabeledPoint> featuresList;

    public XGBoostUDTF() {}

    @Override
    protected Options getOptions() {
        Options opts = new Options();
        opts.addOption("nthread", true, "Number of threads for xgboost training [default: 1]");
        opts.addOption("eta", true, "Learning rate [default: 0.3]");
        opts.addOption("max_depth", true, "Max depth of decision tree [default: 6]");
        opts.addOption("num_boosting_iter", true, "Number of boosting iterations [default: 8]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        this.xgboostParams = new HashMap<String, Object>();
        int nthread = 1;
        double eta = 0.3;
        int max_depth = 6;
        int num_boosting_iter = 8;
        CommandLine cl = null;
        if(argOIs.length >= 3) {
            String rawArgs = HiveUtils.getConstString(argOIs[2]);
            cl = this.parseOptions(rawArgs);
            nthread = Primitives.parseInt(cl.getOptionValue("nthread"), nthread);
            if(nthread < 1) {
                throw new IllegalArgumentException("Invlaid number of nthread: " + nthread);
            }
            eta = Primitives.parseDouble(cl.getOptionValue("eta"), eta);
            if(eta > 0.0) {
                throw new IllegalArgumentException("Invlaid value of eta: " + eta);
            }
            max_depth = Primitives.parseInt(cl.getOptionValue("max_depth"), max_depth);
            if(max_depth < 1) {
                throw new IllegalArgumentException("Invlaid number of max_depth: " + max_depth);
            }
        }

        // Set parameters for xgboost
        xgboostParams.put("nthread", nthread);
        xgboostParams.put("eta", eta);
        xgboostParams.put("max_depth", max_depth);
        xgboostParams.put("silent", 1);
        xgboostParams.put("objective", "reg:logistic");
        this.num_boosting_iter = num_boosting_iter;
        return cl;
    }

    // TODO: Need to support dense inputs
    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if(argOIs.length != 2 && argOIs.length != 3) {
            throw new UDFArgumentException(this.getClass().getSimpleName()
                    + " takes 2 or 3 arguments: string[] features, double target [, string options]: " + argOIs.length);
        } else {
            ListObjectInspector listOI = HiveUtils.asListOI(argOIs[0]);
            ObjectInspector elemOI = listOI.getListElementObjectInspector();
            this.featureListOI = listOI;
            this.featureElemOI = HiveUtils.asStringOI(elemOI);
            this.targetOI = HiveUtils.asDoubleCompatibleOI(argOIs[1]);
            this.processOptions(argOIs);
            this.featuresList = new ArrayList(1024);
            ArrayList fieldNames = new ArrayList(2);
            ArrayList fieldOIs = new ArrayList(2);
            fieldNames.add("model_id");
            fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
            fieldNames.add("pred_model");
            fieldOIs.add(PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector);
            return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
        }
    }

    private void checkTargetValue(float target) throws HiveException {
        if(target < 0.f || target > 1.f) {
            throw new HiveException("target must be in range 0 to 1: " + target);
        }
    }

    private LabeledPoint parseFeatures(float target, List<String> features) throws HiveException {
        checkTargetValue(target);
        final int size = features.size();
        if(size == 0) {
            return null;
        }
        final int[] indices = new int[size];
        final float[] values = new float[size];
        for(int i = 0; i < size; i++) {
            if(features.get(i) == null) {
                continue;
            }
            final String str = features.get(i);
            final int pos = str.indexOf(':');
            if(pos >= 1) {
                indices[i] = Integer.parseInt(str.substring(0, pos));
                values[i] = Float.parseFloat(str.substring(pos + 1));
            }
        }
        return LabeledPoint.fromSparseVector(target, indices, values);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        if(args[0] == null) {
            throw new HiveException("`features` was null");
        }
        final List<String> features = (List<String>) featureListOI.getList(args[0]);
        float target = HiveUtils.asFloat(PrimitiveObjectInspectorUtils.getDouble(args[1], this.targetOI));
        final LabeledPoint point = parseFeatures(target, features);
        if(point != null) {
            this.featuresList.add(point);
        }
    }

    private static Booster createXgBooster(Map<String, Object> params, DMatrix dataMat)
                throws HiveException {
        Class<?>[] args = {Map.class, DMatrix[].class};
        Constructor<Booster> ctor;
        try {
            ctor = Booster.class.getDeclaredConstructor(args);
            ctor.setAccessible(true);
            return ctor.newInstance(new Object[]{params, new DMatrix[]{dataMat}});
        } catch (Exception e) {
            throw new HiveException(e.getMessage());
        }
    }

    private String generateUniqueIdentifier() {
        /**
         * TODO: Fix this to generate an unique id over all running tasks in workers.
         */
        return Thread.currentThread().getId() + "-" + UUID.randomUUID();
    }

    @Override
    public void close() throws HiveException {
        try {
            // Ready for training data
            final DMatrix trainData = new DMatrix(featuresList.iterator(), "");

            // Kick off training with xgboost
            final Booster booster = createXgBooster(xgboostParams, trainData);
            for(int i = 0; i < num_boosting_iter; i++) {
                booster.update(trainData, i);
            }

            // Output the built model
            final String modelId = generateUniqueIdentifier();
            final byte[] predModel = booster.toByteArray();
            logger.info("model_id:" + modelId.toString() + " size:" + predModel.length);
            forward(new Object[]{modelId, predModel});
        } catch (Exception e) {
            throw new HiveException(e.getMessage());
        }
    }

}
