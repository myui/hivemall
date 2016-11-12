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
package hivemall.ftvec;

import static hivemall.utils.hadoop.WritableUtils.val;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;

@Description(name = "extract_feature",
        value = "_FUNC_(feature_vector in array<string>) - Returns features in array<string>")
@UDFType(deterministic = true, stateful = false)
public class ExtractFeatureUDF extends UDF {

    public Text evaluate(String featureVector) {
        if (featureVector == null) {
            return null;
        }
        return val(extractFeature(featureVector));
    }

    public List<Text> evaluate(List<String> featureVectors) {
        if (featureVectors == null) {
            return null;
        }
        final int size = featureVectors.size();
        final Text[] output = new Text[size];
        for (int i = 0; i < size; i++) {
            String fv = featureVectors.get(i);
            if (fv != null) {
                output[i] = new Text(extractFeature(fv));
            }
        }
        return Arrays.asList(output);
    }

    public static String extractFeature(final String ftvec) {
        int pos = ftvec.indexOf(":");
        if (pos > 0) {
            return ftvec.substring(0, pos);
        } else {
            return ftvec;
        }
    }

}
