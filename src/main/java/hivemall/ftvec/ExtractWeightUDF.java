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
package hivemall.ftvec;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.FloatWritable;

@Description(name = "extract_weight", value = "_FUNC_(feature_vector in array<string>) - Returns the weights of features in array<string>")
@UDFType(deterministic = true, stateful = false)
public final class ExtractWeightUDF extends UDF {

    public FloatWritable evaluate(String featureVector) throws UDFArgumentException {
        return extractWeights(featureVector);
    }

    public List<FloatWritable> evaluate(List<String> featureVectors) throws UDFArgumentException {
        if(featureVectors == null) {
            return null;
        }
        final int size = featureVectors.size();
        final FloatWritable[] output = new FloatWritable[size];
        for(int i = 0; i < size; i++) {
            String ftvec = featureVectors.get(i);
            output[i] = extractWeights(ftvec);
        }
        return Arrays.asList(output);
    }

    private static FloatWritable extractWeights(String ftvec) throws UDFArgumentException {
        if(ftvec == null) {
            return null;
        }
        String[] splits = ftvec.split(":");
        if(splits.length != 2) {
            throw new UDFArgumentException("Unexpected feature vector representation: " + ftvec);
        }
        float f = Float.valueOf(splits[1]);
        return new FloatWritable(f);
    }

}
