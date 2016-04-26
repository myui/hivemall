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

public class StringFeature extends Feature {

    @Nonnull
    protected String feature;

    public StringFeature(@Nonnull String feature, double value) {
        super(value);
        this.feature = feature;
    }

    public StringFeature(@Nonnull ByteBuffer src) {
        super();
        readFrom(src);
    }

    StringFeature(int feature, double value) {
        super(value);
        this.feature = Integer.toString(feature);
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
    public int bytes() {
        return NIOUtils.requiredBytes(feature) + Double.SIZE / 8;
    }

    @Override
    public void writeTo(@Nonnull ByteBuffer dst) {
        NIOUtils.putString(feature, dst);
        dst.putDouble(value);
    }

    @Override
    public void readFrom(@Nonnull ByteBuffer src) {
        this.feature = NIOUtils.getString(src);
        this.value = src.getDouble();
    }

    @Override
    public String toString() {
        return feature + ':' + value;
    }

    public static final class StringFeatureWithField extends StringFeature {

        @Nonnull
        protected String field;

        public StringFeatureWithField(@Nonnull String feature, @Nonnull String field, double value) {
            super(feature, value);
            this.field = field;
        }

        @Override
        public void setField(@Nonnull String f) {
            this.field = f;
        }

        @Override
        public String getField() {
            return field;
        }

        @Override
        public int bytes() {
            return super.bytes() + NIOUtils.requiredBytes(field);
        }

        @Override
        public void writeTo(@Nonnull ByteBuffer dst) {
            super.writeTo(dst);
            NIOUtils.putString(field, dst);
        }

        @Override
        public void readFrom(@Nonnull ByteBuffer src) {
            super.readFrom(src);
            this.field = NIOUtils.getString(src);
        }

        @Override
        public String toString() {
            return feature + ':' + field + ':' + value;
        }

    }

    @Nonnull
    public static String getFeatureOfField(@Nonnull Feature x, @Nonnull String yField) {
        return x.getFeature() + ':' + yField;
    }

}
