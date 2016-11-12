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
package hivemall.utils.codec;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import javax.annotation.Nonnull;

/**
 * @link https://en.wikipedia.org/wiki/LEB128
 */
public final class ZigZagLEB128Codec {

    private ZigZagLEB128Codec() {}

    public static int encode(final int n) {
        return (n << 1) ^ (n >> 31);
    }

    public static long encode(final long n) {
        return (n << 1) ^ (n >> 63);
    }

    public static int decode(final int n) {
        return (n >>> 1) ^ -(n & 1);
    }

    public static long decode(final long n) {
        return (n >>> 1) ^ -(n & 1);
    }

    public static void writeSignedInt(final int value, @Nonnull final DataOutput out)
            throws IOException {
        writeUnsignedInt(encode(value), out);
    }

    public static void writeUnsignedInt(int value, @Nonnull final DataOutput out)
            throws IOException {
        while ((value & ~0x7F) != 0L) {
            out.writeByte((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        out.writeByte(value & 0x7F);
    }

    public static void writeSignedLong(final long value, @Nonnull final DataOutput out)
            throws IOException {
        writeUnsignedLong(encode(value), out);
    }

    private static void writeUnsignedLong(long value, @Nonnull final DataOutput out)
            throws IOException {
        while ((value & ~0x7FL) != 0L) {
            out.writeByte(((int) value & 0x7F) | 0x80);
            value >>>= 7;
        }
        out.writeByte((int) value & 0x7F);
    }

    public static int readSignedInt(@Nonnull final DataInput in) throws IOException {
        int raw = readUnsignedInt(in);
        int temp = (((raw << 31) >> 31) ^ raw) >> 1;
        return temp ^ (raw & (1 << 31));
    }

    public static int readUnsignedInt(@Nonnull final DataInput in) throws IOException {
        int value = 0;
        int i = 0;
        int b;
        while (((b = in.readByte()) & 0x80) != 0) {
            value |= (b & 0x7F) << i;
            i += 7;
            if (i > 35) {
                throw new IllegalArgumentException("Variable length quantity is too long: " + i);
            }
        }
        return value | (b << i);
    }

    public static long readSignedLong(@Nonnull final DataInput in) throws IOException {
        long raw = readUnsignedLong(in);
        long temp = (((raw << 63) >> 63) ^ raw) >> 1;
        return temp ^ (raw & (1L << 63));
    }

    public static long readUnsignedLong(@Nonnull final DataInput in) throws IOException {
        long value = 0L;
        int i = 0;
        long b;
        while (((b = in.readByte()) & 0x80L) != 0) {
            value |= (b & 0x7F) << i;
            i += 7;
            if (i > 63) {
                throw new IllegalArgumentException("Variable length quantity is too long: " + i);
            }
        }
        return value | (b << i);
    }

    @Deprecated
    public static void writeFloat(final float value, final DataOutput out) throws IOException {
        int bits = Float.floatToIntBits(value);
        writeSignedInt(bits, out);
    }

    @Deprecated
    public static float readFloat(@Nonnull final DataInput in) throws IOException {
        int bits = readSignedInt(in);
        return Float.intBitsToFloat(bits);
    }

    @Deprecated
    public static void writeDouble(final double value, final DataOutput out) throws IOException {
        long bits = Double.doubleToLongBits(value);
        writeSignedLong(bits, out);
    }

    @Deprecated
    public static double readDouble(@Nonnull final DataInput in) throws IOException {
        long bits = readSignedLong(in);
        return Double.longBitsToDouble(bits);
    }

}
