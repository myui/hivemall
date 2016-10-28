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
package hivemall.ftvec.pairing;

import hivemall.model.FeatureValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;

@Description(name = "powered_features",
        value = "_FUNC_(feature_vector in array<string>, int degree [, boolean truncate])"
                + " - Returns a feature vector having a powered feature space")
@UDFType(deterministic = true, stateful = false)
public final class PoweredFeaturesUDF extends UDF {

    public List<Text> evaluate(final List<Text> ftvec, final int degree) throws HiveException {
        return evaluate(ftvec, degree, true);
    }

    public List<Text> evaluate(final List<Text> ftvec, final int degree, final boolean truncate)
            throws HiveException {
        if (ftvec == null) {
            return null;
        }
        if (degree < 2) {
            throw new HiveException("degree must be greater than or equals to 2: " + degree);
        }
        final int origSize = ftvec.size();
        if (origSize == 0) {
            return Collections.emptyList();
        }

        final FeatureValue probe = new FeatureValue();
        final List<Text> dstVec = new ArrayList<Text>(origSize * degree);
        for (int i = 0; i < origSize; i++) {
            Text t = ftvec.get(i);
            if (t == null) {
                continue;
            }
            dstVec.add(t); // x^1

            FeatureValue.parseFeatureAsString(t, probe);
            final float v = probe.getValueAsFloat();
            if (truncate && (v == 0.f || v == 1.f)) {
                continue;
            }

            final String f = probe.getFeature();
            float baseV = v;
            for (int d = 2; d <= degree; d++) {
                String f2 = f + '^' + d;
                baseV = baseV * v;
                Text t2 = new Text(f2 + ':' + baseV);
                dstVec.add(t2);
            }
        }
        return dstVec;
    }

}
