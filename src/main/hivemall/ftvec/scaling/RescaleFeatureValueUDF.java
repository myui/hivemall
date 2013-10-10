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
package hivemall.ftvec.scaling;

import org.apache.hadoop.hive.ql.exec.UDF;

public class RescaleFeatureValueUDF extends UDF {

    public String evaluate(String s, double min, double max) {
        return evaluate(s, (float) min, (float) max);
    }

    public String evaluate(String s, float min, float max) {
        String[] fv = s.split(":");
        if(fv.length != 2) {
            throw new IllegalArgumentException("Invalid feature value representation: " + s);
        }
        float v = Float.parseFloat(fv[1]);
        float scaled_v = RescaleUDF.min_max_normalization(v, min, max);
        return fv[0] + ':' + Float.toString(scaled_v);
    }

}
