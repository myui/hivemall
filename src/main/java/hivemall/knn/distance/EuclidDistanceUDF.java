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
package hivemall.knn.distance;

import hivemall.io.FeatureValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.Text;

@Description(name = "euclid_distance", value = "_FUNC_(ftvec1, ftvec2) - Returns the square root of the sum of the squared differences")
@UDFType(deterministic = true, stateful = false)
public final class EuclidDistanceUDF extends UDF {

    public DoubleWritable evaluate(final List<Text> ftvec1, final List<Text> ftvec2) {
        final FeatureValue probe = new FeatureValue();
        final Map<String, Float> map = new HashMap<String, Float>(ftvec1.size() * 2 + 1);
        for(Text ft : ftvec1) {
            if(ft == null) {
                continue;
            }
            String s = ft.toString();
            FeatureValue.parseFeatureAsString(s, probe);
            float v1 = probe.getValue();
            String f1 = probe.getFeature();
            map.put(f1, v1);
        }
        double d = 0.d;
        for(Text ft : ftvec2) {
            if(ft == null) {
                continue;
            }
            String s = ft.toString();
            FeatureValue.parseFeatureAsString(s, probe);
            String f2 = probe.getFeature();
            float v2f = probe.getValue();
            Float v1 = map.remove(f2);
            if(v1 == null) {
                d += (v2f * v2f);
            } else {
                float v1f = v1.floatValue();
                float diff = v1f - v2f;
                d += (diff * diff);
            }
        }
        for(Map.Entry<String, Float> e : map.entrySet()) {
            float v1f = e.getValue();
            d += (v1f * v1f);
        }
        return new DoubleWritable(Math.sqrt(d));
    }

}
