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
import hivemall.utils.hadoop.HiveUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.FloatWritable;

@Description(name = "minkowski_distance", value = "_FUNC_(list x, list y, double p) - Returns sum(|x - y|^p)^(1/p)")
@UDFType(deterministic = true, stateful = false)
public final class MinkowskiDistanceUDF extends GenericUDF {

    private ListObjectInspector arg0ListOI, arg1ListOI;
    private double order_p;

    @Override
    public ObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if(argOIs.length != 3) {
            throw new UDFArgumentException("minkowski_distance takes 3 arguments");
        }
        this.arg0ListOI = HiveUtils.asListOI(argOIs[0]);
        this.arg1ListOI = HiveUtils.asListOI(argOIs[1]);
        this.order_p = HiveUtils.getAsConstDouble(argOIs[2]);

        return PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
    }

    @Override
    public FloatWritable evaluate(DeferredObject[] arguments) throws HiveException {
        List<String> ftvec1 = HiveUtils.asStringList(arguments[0], arg0ListOI);
        List<String> ftvec2 = HiveUtils.asStringList(arguments[1], arg1ListOI);
        float d = (float) minkowskiDistance(ftvec1, ftvec2, order_p);
        return new FloatWritable(d);
    }

    public static double minkowskiDistance(final List<String> ftvec1, final List<String> ftvec2, final double orderP) {
        final FeatureValue probe = new FeatureValue();
        final Map<String, Float> map = new HashMap<String, Float>(ftvec1.size() * 2 + 1);
        for(String ft : ftvec1) {
            if(ft == null) {
                continue;
            }
            FeatureValue.parseFeatureAsString(ft, probe);
            float v1 = probe.getValue();
            String f1 = probe.getFeature();
            map.put(f1, v1);
        }
        double d = 0.d;
        for(String ft : ftvec2) {
            if(ft == null) {
                continue;
            }
            FeatureValue.parseFeatureAsString(ft, probe);
            String f2 = probe.getFeature();
            float v2f = probe.getValue();
            Float v1 = map.remove(f2);
            if(v1 == null) {
                d += Math.abs(v2f);
            } else {
                float v1f = v1.floatValue();
                d += Math.pow(Math.abs(v1f - v2f), orderP);
            }
        }
        for(Map.Entry<String, Float> e : map.entrySet()) {
            float v1f = e.getValue();
            d += Math.pow(Math.abs(v1f), orderP);
        }
        return Math.pow(d, 1.d / orderP);
    }

    @Override
    public String getDisplayString(String[] children) {
        return "minkowski_distance(" + Arrays.toString(children) + ")";
    }

}
