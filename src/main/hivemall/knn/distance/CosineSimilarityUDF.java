/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013-2014
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
package hivemall.knn.distance;

import static hivemall.utils.hadoop.WritableUtils.val;
import hivemall.io.FeatureValue;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.FloatWritable;

@UDFType(deterministic = true, stateful = false)
public final class CosineSimilarityUDF extends UDF {

    public FloatWritable evaluate(List<String> ftvec1, List<String> ftvec2, boolean noWeight) {
        if(ftvec1 == null || ftvec2 == null) {
            return val(0.f);
        }

        final Map<String, Float> map1 = new HashMap<String, Float>(ftvec1.size() * 2 + 1);
        double score1 = 0.d;
        for(String ft : ftvec1) {
            if(noWeight) {
                score1++;
                map1.put(ft, 1.f);
            } else {
                FeatureValue fv = FeatureValue.parseFeatureAsString(ft);
                float v = fv.getValue();
                score1 += (v * v);
                String f = fv.getFeature();
                map1.put(f, v);
            }
        }
        double l1norm1 = Math.sqrt(score1);

        float dotp = 0.f;
        double score2 = 0.d;
        for(String ft : ftvec2) {
            if(noWeight) {
                score2++;
                if(map1.containsKey(ft)) {
                    dotp++;
                }
            } else {
                FeatureValue fv = FeatureValue.parseFeatureAsString(ft);
                float v2 = fv.getValue();
                score2 += (v2 * v2);
                String f2 = fv.getFeature();
                Float v1 = map1.get(f2);
                if(v1 != null) {
                    dotp += (v1.floatValue() * v2);
                }
            }
        }
        double l1norm2 = Math.sqrt(score2);

        final double denom = (l1norm1 * l1norm2);
        if(denom <= 0.f) {
            return val(0.f);
        } else {
            return val((float) (dotp / denom));
        }
    }

    public FloatWritable evaluate(List<Integer> ftvec1, List<Integer> ftvec2) {
        if(ftvec1 == null || ftvec2 == null) {
            return val(0.f);
        }

        Collections.sort(ftvec1);

        double dotp = 0.f;
        for(Integer f : ftvec2) {
            if(Collections.binarySearch(ftvec1, f) >= 0) {
                dotp++;
            }
        }

        double l1norm1 = Math.sqrt(ftvec1.size());
        double l1norm2 = Math.sqrt(ftvec2.size());

        final double denom = (l1norm1 * l1norm2);
        if(denom <= 0.f) {
            return val(0.f);
        } else {
            return val((float) (dotp / denom));
        }
    }

}
