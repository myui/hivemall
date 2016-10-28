/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package hivemall.ftvec.trans;

import hivemall.UDFWithOptions;
import hivemall.fm.Feature;
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.hashing.MurmurHash3;
import hivemall.utils.lang.Primitives;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.Text;

@Description(
        name = "ffm_features",
        value = "_FUNC_(const array<string> featureNames, feature1, feature2, .. [, const string options])"
                + " - Takes categroical variables and returns a feature vector array<string>"
                + " in a libffm format <field>:<index>:<value>")
@UDFType(deterministic = true, stateful = false)
public final class FFMFeaturesUDF extends UDFWithOptions {

    private String[] _featureNames;
    private PrimitiveObjectInspector[] _inputOIs;
    private List<Text> _result;

    private boolean _mhash = true;
    private int _numFeatures = Feature.DEFAULT_NUM_FEATURES;
    private int _numFields = Feature.DEFAULT_NUM_FIELDS;

    @Override
    protected Options getOptions() {
        Options opts = new Options();
        opts.addOption("no_hash", "disable_feature_hashing", false,
            "Wheather to disable feature hashing [default: false]");
        // feature hashing
        opts.addOption("hash", "feature_hashing", true,
            "The number of bits for feature hashing in range [18,31] [default:21]");
        opts.addOption("fields", "num_fields", true, "The number of fields [default:1024]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(@Nonnull String optionValue) throws UDFArgumentException {
        CommandLine cl = parseOptions(optionValue);

        // feature hashing
        int hashbits = Primitives.parseInt(cl.getOptionValue("feature_hashing"),
            Feature.DEFAULT_FEATURE_BITS);
        if (hashbits < 18 || hashbits > 31) {
            throw new UDFArgumentException("-feature_hashing MUST be in range [18,31]: " + hashbits);
        }
        int numFeatures = 1 << hashbits;
        int numFields = Primitives.parseInt(cl.getOptionValue("num_fields"),
            Feature.DEFAULT_NUM_FIELDS);
        if (numFields <= 1) {
            throw new UDFArgumentException("-num_fields MUST be greater than 1: " + numFields);
        }
        this._numFeatures = numFeatures;
        this._numFields = numFields;
        return cl;
    }

    @Override
    public ObjectInspector initialize(@Nonnull final ObjectInspector[] argOIs)
            throws UDFArgumentException {
        final int numArgOIs = argOIs.length;
        if (numArgOIs < 2) {
            throw new UDFArgumentLengthException(
                "the number of arguments must be greater that or equals to 2: " + numArgOIs);
        }
        this._featureNames = HiveUtils.getConstStringArray(argOIs[0]);
        if (_featureNames == null) {
            throw new UDFArgumentException("#featureNames should not be null");
        }
        int numFeatureNames = _featureNames.length;
        if (numFeatureNames < 1) {
            throw new UDFArgumentException("#featureNames must be greater than or equals to 1: "
                    + numFeatureNames);
        }
        for (String featureName : _featureNames) {
            if (featureName.indexOf(':') != -1) {
                throw new UDFArgumentException("featureName should not include colon: "
                        + featureName);
            }
        }

        final int numFeatures;
        final int lastArgIndex = numArgOIs - 1;
        if (lastArgIndex > numFeatureNames) {
            if (lastArgIndex == (numFeatureNames + 1)
                    && HiveUtils.isConstString(argOIs[lastArgIndex])) {
                String optionValue = HiveUtils.getConstString(argOIs[lastArgIndex]);
                processOptions(optionValue);
                numFeatures = numArgOIs - 2;
            } else {
                throw new UDFArgumentException(
                    "Unexpected arguments for _FUNC_"
                            + "(const array<string> featureNames, feature1, feature2, .. [, const string options])");
            }
        } else {
            numFeatures = lastArgIndex;
        }
        if (numFeatureNames != numFeatures) {
            throw new UDFArgumentLengthException("#featureNames '" + numFeatureNames
                    + "' != #features '" + numFeatures + "'");
        }

        this._inputOIs = new PrimitiveObjectInspector[numFeatures];
        for (int i = 0; i < numFeatures; i++) {
            ObjectInspector oi = argOIs[i + 1];
            _inputOIs[i] = HiveUtils.asPrimitiveObjectInspector(oi);
        }
        this._result = new ArrayList<Text>(numFeatures);

        return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }

    @Override
    public List<Text> evaluate(@Nonnull final DeferredObject[] arguments) throws HiveException {
        _result.clear();

        final StringBuilder builder = new StringBuilder(128);
        final int size = _featureNames.length;
        for (int i = 0; i < size; i++) {
            Object argument = arguments[i + 1].get();
            if (argument == null) {
                continue;
            }

            PrimitiveObjectInspector oi = _inputOIs[i];
            String s = PrimitiveObjectInspectorUtils.getString(argument, oi);
            if (s.isEmpty()) {
                continue;
            }
            if (s.indexOf(':') != -1) {
                throw new HiveException("feature index SHOULD NOT include colon: " + s);
            }

            final String featureName = _featureNames[i];
            final String feature = featureName + '#' + s;
            // categorical feature representation 
            final String fv;
            if (_mhash) {
                int field = MurmurHash3.murmurhash3(_featureNames[i], _numFields);
                // +NUM_FIELD to avoid conflict to quantitative features
                int index = MurmurHash3.murmurhash3(feature, _numFeatures) + _numFields;
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

            _result.add(new Text(fv));
        }
        return _result;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "ffm_features(" + Arrays.toString(children) + ")";
    }

}
