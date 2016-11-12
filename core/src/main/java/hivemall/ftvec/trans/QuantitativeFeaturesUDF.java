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

import hivemall.utils.hadoop.HiveUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.Text;

@Description(name = "quantitative_features",
        value = "_FUNC_(array<string> featureNames, ...) - Returns a feature vector array<string>")
@UDFType(deterministic = true, stateful = false)
public final class QuantitativeFeaturesUDF extends GenericUDF {

    private String[] featureNames;
    private PrimitiveObjectInspector[] inputOIs;
    private List<Text> result;

    @Override
    public ObjectInspector initialize(@Nonnull final ObjectInspector[] argOIs)
            throws UDFArgumentException {
        final int numArgOIs = argOIs.length;
        if (numArgOIs < 2) {
            throw new UDFArgumentException("argOIs.length must be greater that or equals to 2: "
                    + numArgOIs);
        }
        this.featureNames = HiveUtils.getConstStringArray(argOIs[0]);
        if (featureNames == null) {
            throw new UDFArgumentException("#featureNames should not be null");
        }
        int numFeatureNames = featureNames.length;
        if (numFeatureNames < 1) {
            throw new UDFArgumentException("#featureNames must be greater than or equals to 1: "
                    + numFeatureNames);
        }
        int numFeatures = numArgOIs - 1;
        if (numFeatureNames != numFeatures) {
            throw new UDFArgumentException("#featureNames '" + numFeatureNames
                    + "' != #arguments '" + numFeatures + "'");
        }

        this.inputOIs = new PrimitiveObjectInspector[numFeatures];
        for (int i = 0; i < numFeatures; i++) {
            ObjectInspector oi = argOIs[i + 1];
            inputOIs[i] = HiveUtils.asDoubleCompatibleOI(oi);
        }
        this.result = new ArrayList<Text>(numFeatures);

        return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }

    @Override
    public List<Text> evaluate(@Nonnull final DeferredObject[] arguments) throws HiveException {
        result.clear();

        final int size = arguments.length - 1;
        for (int i = 0; i < size; i++) {
            Object argument = arguments[i + 1].get();
            if (argument == null) {
                continue;
            }

            PrimitiveObjectInspector oi = inputOIs[i];
            if (oi.getPrimitiveCategory() == PrimitiveCategory.STRING) {
                String s = argument.toString();
                if (s.isEmpty()) {
                    continue;
                }
            }

            final double v = PrimitiveObjectInspectorUtils.getDouble(argument, oi);
            if (v != 0.d) {
                String featureName = featureNames[i];
                Text f = new Text(featureName + ':' + v);
                result.add(f);
            }
        }
        return result;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "quantitative_features(" + Arrays.toString(children) + ")";
    }

}
