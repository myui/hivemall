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
package hivemall.io;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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

    @Nullable
    public static FeatureValue parse(final Object o) throws IllegalArgumentException {
        if(o == null) {
            return null;
        }
        String s = o.toString();
        return parse(s);
    }

    @Nullable
    public static FeatureValue parse(@Nonnull final String s) throws IllegalArgumentException {
        assert (s != null);
        final int pos = s.indexOf(':');
        if(pos == 0) {
            throw new IllegalArgumentException("Invalid feature value representation: " + s);
        }

        final Text feature;
        final float weight;
        if(pos > 0) {
            String s1 = s.substring(0, pos);
            String s2 = s.substring(pos + 1);
            feature = new Text(s1);
            weight = Float.parseFloat(s2);
        } else {
            feature = new Text(s);
            weight = 1.f;
        }
        return new FeatureValue(feature, weight);
    }

    @Nonnull
    public static FeatureValue parseFeatureAsString(@Nonnull final String s)
            throws IllegalArgumentException {
        assert (s != null);
        final int pos = s.indexOf(':');
        if(pos == 0) {
            throw new IllegalArgumentException("Invalid feature value representation: " + s);
        }

        final String feature;
        final float weight;
        if(pos > 0) {
            feature = s.substring(0, pos);
            String s2 = s.substring(pos + 1);
            weight = Float.parseFloat(s2);
        } else {
            feature = s;
            weight = 1.f;
        }
        return new FeatureValue(feature, weight);
    }

}
