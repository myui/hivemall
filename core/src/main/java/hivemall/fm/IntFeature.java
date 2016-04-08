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

import java.nio.ByteBuffer;

import javax.annotation.Nonnull;

public final class IntFeature extends Feature {
    private int index;

    public IntFeature(int index, double value) {
        super(value);
        this.index = index;
    }

    public IntFeature(@Nonnull ByteBuffer src) {
        super();
        readFrom(src);
    }

    @Override
    public String getFeature() {
        return Integer.toString(index);
    }

    @Override
    public int getFeatureIndex() {
        return index;
    }

    @Override
    public void setFeatureIndex(int i) {
        this.index = i;
    }

    @Override
    public int bytes() {
        return (Integer.SIZE + Double.SIZE) / 8;
    }

    @Override
    public void writeTo(@Nonnull final ByteBuffer dst) {
        dst.putInt(index);
        dst.putDouble(value);
    }

    @Override
    public void readFrom(@Nonnull final ByteBuffer src) {
        this.index = src.getInt();
        this.value = src.getDouble();
    }

    @Override
    public String toString() {
        return index + ":" + value;
    }

}
