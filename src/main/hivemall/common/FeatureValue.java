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
package hivemall.common;

import hivemall.ftvec.hashing.MurmurHash3UDF;

import org.apache.hadoop.io.Text;

public final class FeatureValue {

    private final Object feature;
    private final float value;

    public FeatureValue(Object f, float v) {
        this.feature = f;
        this.value = v;
    }

    @SuppressWarnings("unchecked")
    public <T> T getFeature() {
        return (T) feature;
    }

    public float getValue() {
        return value;
    }

    public static FeatureValue parse(Object o, boolean feature_hashing) {
        if(o == null) {
            return null;
        }
        String s = o.toString();
        if(feature_hashing) {
            int hashval = MurmurHash3UDF.murmurhash3(s);
            return new FeatureValue(Integer.valueOf(hashval), 1.f);
        }
        return parse(s);
    }

    public static FeatureValue parse(String s) {
        if(s == null) {
            return null;
        }
        String[] fv = s.split(":");
        if(fv.length != 1 && fv.length != 2) {
            throw new IllegalArgumentException("Invalid feature value representation: " + s);
        }
        Text f = new Text(fv[0]);
        float v = (fv.length == 1) ? 1.f : Float.parseFloat(fv[1]);
        return new FeatureValue(f, v);
    }

}
