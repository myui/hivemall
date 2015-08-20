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
package hivemall.knn.similarity;

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

@Description(name = "cosine_similarity", value = "_FUNC_(ftvec1, ftvec2) - Returns a cosine similarity of the given two vectors")
@UDFType(deterministic = true, stateful = false)
public final class CosineSimilarityUDF extends GenericUDF {

    private ListObjectInspector arg0ListOI, arg1ListOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if(argOIs.length != 2) {
            throw new UDFArgumentException("cosine_similarity takes 2 arguments");
        }
        this.arg0ListOI = HiveUtils.asListOI(argOIs[0]);
        this.arg1ListOI = HiveUtils.asListOI(argOIs[1]);

        return PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
    }

    public FloatWritable evaluate(List<String> ftvec1, List<String> ftvec2) {
        return new FloatWritable(cosineSimilarity(ftvec1, ftvec2));
    }

    @Override
    public FloatWritable evaluate(DeferredObject[] arguments) throws HiveException {
        List<String> ftvec1 = HiveUtils.asStringList(arguments[0], arg0ListOI);
        List<String> ftvec2 = HiveUtils.asStringList(arguments[1], arg1ListOI);
        float similarity = cosineSimilarity(ftvec1, ftvec2);
        return new FloatWritable(similarity);
    }

    public static float cosineSimilarity(final List<String> ftvec1, final List<String> ftvec2) {
        if(ftvec1 == null || ftvec2 == null) {
            return 0.f;
        }

        final FeatureValue probe = new FeatureValue();
        final Map<String, Float> map1 = new HashMap<String, Float>(ftvec1.size() * 2 + 1);
        double score1 = 0.d;
        for(String ft : ftvec1) {
            FeatureValue.parseFeatureAsString(ft, probe);
            float v = probe.getValue();
            score1 += (v * v);
            String f = probe.getFeature();
            map1.put(f, v);
        }
        double l1norm1 = Math.sqrt(score1);

        float dotp = 0.f;
        double score2 = 0.d;
        for(String ft : ftvec2) {
            FeatureValue.parseFeatureAsString(ft, probe);
            float v2 = probe.getValue();
            score2 += (v2 * v2);
            String f2 = probe.getFeature();
            Float v1 = map1.get(f2);
            if(v1 != null) {
                dotp += (v1.floatValue() * v2);
            }
        }
        double l1norm2 = Math.sqrt(score2);

        final double denom = l1norm1 * l1norm2;
        if(denom <= 0.f) {
            return 0.f;
        } else {
            return (float) (dotp / denom);
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return "consine_similarity(" + Arrays.toString(children) + ")";
    }

}
