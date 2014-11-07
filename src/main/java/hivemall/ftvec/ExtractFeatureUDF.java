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

import static hivemall.utils.hadoop.WritableUtils.val;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;

@Description(name = "extract_feature", value = "_FUNC_(feature_vector in array<string>) - Returns features in array<string>")
@UDFType(deterministic = true, stateful = false)
public class ExtractFeatureUDF extends UDF {

    public Text evaluate(String featureVector) {
        if(featureVector == null) {
            return null;
        }
        return val(extractFeature(featureVector));
    }

    public List<Text> evaluate(List<String> featureVectors) {
        if(featureVectors == null) {
            return null;
        }
        final int size = featureVectors.size();
        final Text[] output = new Text[size];
        for(int i = 0; i < size; i++) {
            String fv = featureVectors.get(i);
            if(fv != null) {
                output[i] = new Text(extractFeature(fv));
            }
        }
        return Arrays.asList(output);
    }

    public static String extractFeature(final String ftvec) {
        int pos = ftvec.indexOf(":");
        if(pos > 0) {
            return ftvec.substring(0, pos);
        } else {
            return ftvec;
        }
    }

}
