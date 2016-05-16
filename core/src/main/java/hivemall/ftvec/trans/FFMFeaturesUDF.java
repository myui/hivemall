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
package hivemall.ftvec.trans;

import hivemall.fm.Feature;
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.hashing.MurmurHash3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.Text;

@Description(name = "ffm_features",
        value = "_FUNC_(boolean mhash, const array<string> featureNames, feature1, feature2, ..)"
                + " - Takes categroical variables and returns a feature vector array<string>"
                + " in a libffm format <field>:<index>:<value>")
@UDFType(deterministic = true, stateful = false)
public final class FFMFeaturesUDF extends GenericUDF {

    private boolean mhash;
    private String[] featureNames;
    private PrimitiveObjectInspector[] inputOIs;
    private List<Text> result;

    @Override
    public ObjectInspector initialize(@Nonnull final ObjectInspector[] argOIs)
            throws UDFArgumentException {
        final int numArgOIs = argOIs.length;
        if (numArgOIs < 3) {
            throw new UDFArgumentLengthException(
                "the number of arguments must be greater that or equals to 3: " + numArgOIs);
        }
        this.mhash = HiveUtils.getConstBoolean(argOIs[0]);
        this.featureNames = HiveUtils.getConstStringArray(argOIs[1]);
        if (featureNames == null) {
            throw new UDFArgumentException("#featureNames should not be null");
        }
        for (String featureName : featureNames) {
            if (featureName.indexOf(':') != -1) {
                throw new UDFArgumentException("featureName should not include colon: "
                        + featureName);
            }
        }

        int numFeatureNames = featureNames.length;
        if (numFeatureNames < 1) {
            throw new UDFArgumentException("#featureNames must be greater than or equals to 1: "
                    + numFeatureNames);
        }
        int numFeatures = numArgOIs - 2;
        if (numFeatureNames != numFeatures) {
            throw new UDFArgumentLengthException("#featureNames '" + numFeatureNames
                    + "' != #arguments '" + numFeatures + "'");
        }

        this.inputOIs = new PrimitiveObjectInspector[numFeatures];
        for (int i = 0; i < numFeatures; i++) {
            ObjectInspector oi = argOIs[i + 2];
            inputOIs[i] = HiveUtils.asPrimitiveObjectInspector(oi);
        }
        this.result = new ArrayList<Text>(numFeatures);

        return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }

    @Override
    public List<Text> evaluate(@Nonnull final DeferredObject[] arguments) throws HiveException {
        result.clear();

        final StringBuilder builder = new StringBuilder(128);
        final int size = arguments.length - 2;
        for (int i = 0; i < size; i++) {
            Object argument = arguments[i + 2].get();
            if (argument == null) {
                continue;
            }

            PrimitiveObjectInspector oi = inputOIs[i];
            String s = PrimitiveObjectInspectorUtils.getString(argument, oi);
            if (s.isEmpty()) {
                continue;
            }
            if (s.indexOf(':') != -1) {
                throw new HiveException("feature index SHOULD NOT include colon: " + s);
            }

            final String featureName = featureNames[i];
            final String feature = featureName + '#' + s;
            // categorical feature representation 
            final String fv;
            if (mhash) {
                int field = MurmurHash3.murmurhash3(featureNames[i], Feature.NUM_FIELDS);
                // +NUM_FIELD to avoid conflict to quantitative features
                int index = MurmurHash3.murmurhash3(feature, Feature.NUM_FEATURES) + Feature.NUM_FIELDS;
                fv = builder.append(field).append(':').append(index).append(":1").toString();
                builder.setLength(0);
            } else {
                fv = builder.append(featureName)
                            .append(':')
                            .append(feature)
                            .append(":1")
                            .toString();
                builder.setLength(0);
            }

            result.add(new Text(fv));
        }
        return result;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "ffm_features(" + Arrays.toString(children) + ")";
    }

}
