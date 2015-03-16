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
