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
package hivemall.ftvec.scaling;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;

/**
 * @see <a href="http://mathworld.wolfram.com/NormalizedVector.html>http://mathworld.wolfram.com/NormalizedVector.html</a>
 */
@Description(name = "normalize", value = "_FUNC_(ftvec string) - Returned a L2 normalized value")
@UDFType(deterministic = true, stateful = false)
public final class L2NormalizationUDF extends UDF {

    public List<Text> evaluate(final List<Text> ftvecs) {
        if(ftvecs == null) {
            return null;
        }
        double squaredSum = 0d;
        final int numFeatures = ftvecs.size();
        final String[] features = new String[numFeatures];
        final float[] weights = new float[numFeatures];
        for(int i = 0; i < numFeatures; i++) {
            Text ftvec = ftvecs.get(i);
            if(ftvec == null) {
                continue;
            }
            String s = ftvec.toString();
            final String[] ft = s.split(":");
            final int ftlen = ft.length;
            if(ftlen == 1) {
                features[i] = ft[0];
                weights[i] = 1f;
                squaredSum += 1d;
            } else if(ftlen == 2) {
                features[i] = ft[0];               
                float v = Float.parseFloat(ft[1]);
                weights[i] = v;
                squaredSum += (v * v);
            } else {
                throw new IllegalArgumentException("Invalid feature value representation: " + s);
            }
        }
        final float norm = (float) Math.sqrt(squaredSum);
        final Text[] t = new Text[numFeatures];
        for(int i = 0; i < numFeatures; i++) {
            String f = features[i];
            float v = weights[i] / norm;
            t[i] = new Text(f + ':' + v);
        }
        return Arrays.asList(t);
    }

}
