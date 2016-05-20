/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
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
package hivemall.utils.codec;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import javax.annotation.Nonnull;

public final class ZigZagCodec {

    private ZigZagCodec() {}

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

    public static void writeSignedVInt(final int value, @Nonnull final DataOutput out)
            throws IOException {
        writeUnsignedVInt(encode(value), out);
    }

    public static void writeUnsignedVInt(int value, @Nonnull final DataOutput out)
            throws IOException {
        while ((value & ~0x7F) != 0L) {
            out.writeByte((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        out.writeByte(value & 0x7F);
    }

    public static void writeSignedVLong(final long value, @Nonnull final DataOutput out)
            throws IOException {
        writeUnsignedVLong(encode(value), out);
    }

    private static void writeUnsignedVLong(long value, @Nonnull final DataOutput out)
            throws IOException {
        while ((value & ~0x7FL) != 0L) {
            out.writeByte(((int) value & 0x7F) | 0x80);
            value >>>= 7;
        }
        out.writeByte((int) value & 0x7F);
    }

    public static int readSignedVInt(@Nonnull final DataInput in) throws IOException {
        int raw = readUnsignedVInt(in);
        int temp = (((raw << 31) >> 31) ^ raw) >> 1;
        return temp ^ (raw & (1 << 31));
    }

    public static int readUnsignedVInt(@Nonnull final DataInput in) throws IOException {
        int value = 0;
        int i = 0;
        int b;
        while (((b = in.readByte()) & 0x80) != 0) {
            value |= (b & 0x7F) << i;
            i += 7;
            if (i > 35) {
                throw new IllegalArgumentException("Variable length quantity is too long");
            }
        }
        return value | (b << i);
    }

    public static long readSignedVarLong(@Nonnull final DataInput in) throws IOException {
        long raw = readUnsignedVLong(in);
        long temp = (((raw << 63) >> 63) ^ raw) >> 1;
        return temp ^ (raw & (1L << 63));
    }

    public static long readUnsignedVLong(@Nonnull final DataInput in) throws IOException {
        long value = 0L;
        int i = 0;
        long b;
        while (((b = in.readByte()) & 0x80L) != 0) {
            value |= (b & 0x7F) << i;
            i += 7;
            if (i > 63) {
                throw new IllegalArgumentException("Variable length quantity is too long");
            }
        }
        return value | (b << i);
    }

    public static void writeFloat(final float value, final DataOutput out) throws IOException {
        int bits = Float.floatToIntBits(value);
        writeSignedVInt(bits, out);
    }

    public static float readFloat(@Nonnull final DataInput in) throws IOException {
        int bits = readSignedVInt(in);
        return Float.intBitsToFloat(bits);
    }

    public static void writeDouble(final double value, final DataOutput out) throws IOException {
        long bits = Double.doubleToLongBits(value);
        writeSignedVLong(bits, out);
    }

    public static double readDouble(@Nonnull final DataInput in) throws IOException {
        long bits = readSignedVarLong(in);
        return Double.longBitsToDouble(bits);
    }

}
