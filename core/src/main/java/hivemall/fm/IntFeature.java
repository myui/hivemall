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
package hivemall.fm;

import java.nio.ByteBuffer;

import javax.annotation.Nonnull;

public final class IntFeature extends Feature {

    private int index;
    /** -1 if not defined */
    private short field;

    public IntFeature(int index, double value) {
        this(index, (short) -1, value);
    }

    public IntFeature(int index, short field, double value) {
        super(value);
        this.field = field;
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
    public short getField() {
        return field;
    }

    @Override
    public void setField(short field) {
        this.field = field;
    }

    @Override
    public int bytes() {
        return (Integer.SIZE + Short.SIZE + Double.SIZE) / Byte.SIZE;
    }

    @Override
    public void writeTo(@Nonnull final ByteBuffer dst) {
        dst.putInt(index);
        dst.putShort(field);
        dst.putDouble(value);
    }

    @Override
    public void readFrom(@Nonnull final ByteBuffer src) {
        this.index = src.getInt();
        this.field = src.getShort();
        this.value = src.getDouble();
    }

    @Override
    public String toString() {
        if (field == -1) {
            return index + ":" + value;
        } else {
            return index + ":" + field + ":" + value;
        }
    }

}
