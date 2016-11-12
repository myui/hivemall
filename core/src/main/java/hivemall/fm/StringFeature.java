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

    // VisibleForTesting
    StringFeature(int feature, double value) {
        this(String.valueOf(feature), value);
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
    public int bytes() {
        return NIOUtils.requiredBytes(feature) + Double.SIZE / Byte.SIZE;
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

}
