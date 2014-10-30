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
