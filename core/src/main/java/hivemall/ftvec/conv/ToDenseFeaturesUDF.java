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
package hivemall.ftvec.conv;

import hivemall.model.FeatureValue;

import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnegative;
import javax.annotation.Nullable;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;

@Description(name = "to_dense_features",
        value = "_FUNC_(array<string> feature_vector, int dimensions)"
                + " - Returns a dense feature in array<float>")
@UDFType(deterministic = true, stateful = false)
public final class ToDenseFeaturesUDF extends UDF {

    @Nullable
    public List<Float> evaluate(@Nullable final List<String> features) throws HiveException {
        if (features == null) {
            return null;
        }
        int size = features.size();
        return evaluate(features, size);
    }

    @Nullable
    public List<Float> evaluate(@Nullable final List<String> features,
            @Nonnegative final int dimensions) throws HiveException {
        if (features == null) {
            return null;
        }

        final FeatureValue probe = new FeatureValue();
        final Float[] fv = new Float[dimensions + 1];
        for (String ft : features) {
            FeatureValue.parseFeatureAsString(ft, probe);
            String f = probe.getFeature();
            int i = Integer.parseInt(f);
            if (i > dimensions) {
                throw new HiveException("IndexOutOfBounds: " + i);
            }
            float v = probe.getValueAsFloat();
            fv[i] = new Float(v);
        }
        return Arrays.asList(fv);

    }

}
