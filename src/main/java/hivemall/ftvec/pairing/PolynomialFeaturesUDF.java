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
package hivemall.ftvec.pairing;

import hivemall.io.FeatureValue;
import hivemall.utils.HivemallUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;

/**
 * Maps a vector `(x, y, z, ...)` into a polynomial feature space `(x, y, z,
 * x^2, xy, xz, y^2, yz, z^2, x^3, x^2y, x^2z, xyz, ...)^T`.
 */
@Description(name = "polynomial_features", value = "_FUNC_(feature_vector in array<string>) - Returns a feature vector"
        + "having polynominal feature space")
@UDFType(deterministic = true, stateful = false)
public final class PolynomialFeaturesUDF extends UDF {

    public List<Text> evaluate(final List<Text> ftvec, final int degree) throws HiveException {
        return evaluate(ftvec, degree, false, true);
    }

    public List<Text> evaluate(final List<Text> ftvec, final int degree, final boolean interactionOnly)
            throws HiveException {
        return evaluate(ftvec, degree, interactionOnly, true);
    }

    public List<Text> evaluate(final List<Text> ftvec, final int degree, final boolean interactionOnly, final boolean truncate)
            throws HiveException {
        if(ftvec == null) {
            return null;
        }
        if(degree < 2) {
            throw new HiveException("degree must be greater than or equals to 2: " + degree);
        }
        final int origSize = ftvec.size();
        if(origSize == 0) {
            return Collections.emptyList();
        }

        final List<FeatureValue> srcVec = HivemallUtils.parseTextFeaturesAsString(ftvec);

        final List<Text> dstVec = new ArrayList<Text>(origSize * degree * 2);
        for(int i = 0; i < origSize; i++) {
            Text t = ftvec.get(i);
            if(t == null) {
                continue;
            }
            dstVec.add(t); // x^1

            FeatureValue fv = srcVec.get(i);
            float v = fv.getValue();
            if(truncate == false || (v != 0.f && v != 1.f)) {
                String f = fv.getFeature();
                addPolynomialFeature(f, v, 2, degree, srcVec, i, dstVec, interactionOnly, truncate);
            }

        }
        return dstVec;
    }

    private static void addPolynomialFeature(final String baseF, final float baseV, final int currentDegree, final int degree, final List<FeatureValue> srcVec, final int currentSrcPos, final List<Text> dstVec, final boolean interactionOnly, final boolean truncate) {
        assert (currentDegree <= degree) : "currentDegree: " + currentDegree + ", degress: "
                + degree;

        final int lastSrcIndex = srcVec.size() - 1;
        for(int i = currentSrcPos; i <= lastSrcIndex; i++) {
            if(interactionOnly && i == currentSrcPos) {
                continue;
            }

            FeatureValue ftvec = srcVec.get(i);
            float v = ftvec.getValue();
            if(truncate && (v == 0.f || v == 1.f)) {
                continue;
            }
            String f = ftvec.getFeature();

            String f2 = baseF + '^' + f;
            float v2 = baseV * v;
            String fv2 = f2 + ':' + v2;
            dstVec.add(new Text(fv2)); // x^x

            if(currentDegree < degree && i <= lastSrcIndex) {
                addPolynomialFeature(f2, v2, currentDegree + 1, degree, srcVec, i, dstVec, interactionOnly, truncate);
            }
        }
    }

}
