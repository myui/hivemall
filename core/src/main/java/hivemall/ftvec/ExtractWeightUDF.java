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

import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

@Description(
        name = "extract_weight",
        value = "_FUNC_(feature_vector in array<string>) - Returns the weights of features in array<string>")
@UDFType(deterministic = true, stateful = false)
public final class ExtractWeightUDF extends UDF {

    public DoubleWritable evaluate(String featureVector) throws UDFArgumentException {
        return extractWeights(featureVector);
    }

    public List<DoubleWritable> evaluate(List<String> featureVectors) throws UDFArgumentException {
        if (featureVectors == null) {
            return null;
        }
        final int size = featureVectors.size();
        final DoubleWritable[] output = new DoubleWritable[size];
        for (int i = 0; i < size; i++) {
            String ftvec = featureVectors.get(i);
            output[i] = extractWeights(ftvec);
        }
        return Arrays.asList(output);
    }

    static DoubleWritable extractWeights(String ftvec) throws UDFArgumentException {
        if (ftvec == null) {
            return null;
        }

        final int pos = ftvec.lastIndexOf(':');
        if (pos > 0) {
            String s = ftvec.substring(pos + 1);
            double d = parseDouble(s);
            return new DoubleWritable(d);
        } else {
            return new DoubleWritable(1.d);
        }
    }

    private static double parseDouble(@Nonnull final String v) throws UDFArgumentException {
        try {
            return Double.parseDouble(v);
        } catch (NumberFormatException nfe) {
            throw new UDFArgumentException(nfe);
        }
    }

}
