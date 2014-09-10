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
