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
package hivemall.fm;

import hivemall.utils.io.NIOUtils;

import java.nio.ByteBuffer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class StringFeature extends Feature {

    @Nonnull
    protected String feature;
    @Nullable
    protected String field;

    public StringFeature(@Nonnull String feature, double value) {
        this(feature, null, value);
    }

    // VisibleForTesting
    StringFeature(int feature, double value) {
        this(String.valueOf(feature), null, value);
    }

    public StringFeature(@Nonnull String feature, @Nullable String field, double value) {
        super(value);
        this.feature = feature;
        this.field = field;
    }

    public StringFeature(@Nonnull ByteBuffer src) {
        super();
        readFrom(src);
    }

    @Override
    public String getFeature() {
        return feature;
    }

    @Override
    public void setFeature(@Nonnull String feature) {
        this.feature = feature;
    }

    @Override
    public void setField(@Nullable String f) {
        this.field = f;
    }

    @Override
    public String getField() {
        if (field == null) {
            return feature; // CAUTION: <field> equals to <index> for quantitative features
        }
        return field;
    }

    @Override
    public int bytes() {
        return NIOUtils.requiredBytes(feature) + NIOUtils.requiredBytes(field) + Double.SIZE / 8;
    }

    @Override
    public void writeTo(@Nonnull ByteBuffer dst) {
        NIOUtils.putString(feature, dst);
        NIOUtils.putString(field, dst);
        dst.putDouble(value);
    }

    @Override
    public void readFrom(@Nonnull ByteBuffer src) {
        this.feature = NIOUtils.getString(src);
        this.field = NIOUtils.getString(src);
        this.value = src.getDouble();
    }

    @Override
    public String toString() {
        if (field == null) {
            return feature + ':' + value;
        } else {
            return feature + ':' + field + ':' + value;
        }
    }

    @Nonnull
    public static String getFeatureOfField(@Nonnull final Feature x, @Nonnull final String yField) {
        return x.getFeature() + ':' + yField;
    }

}
