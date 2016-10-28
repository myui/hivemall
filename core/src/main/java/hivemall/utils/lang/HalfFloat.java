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
package hivemall.utils.lang;

/**
 * A utility class to deal with half-precision floating-point. The conversion is very fast because
 * there is no conditional branch instruction in the conversion.
 * 
 * <pre>
 * |sign|       exponent          |                   mantissa                                 |
 * | 31 | 30 29 28 27 26 25 24 23 | 22 21 20 19 18 17 16 15 14 13 12 11 10 9 8 7 6 5 4 3 2 1 0 |
 * </pre>
 * 
 * @see http://en.wikipedia.org/wiki/Half-precision_floating-point_format
 * @see http://en.wikipedia.org/wiki/Single_precision_floating-point_format
 * @see ftp://www.fox-toolkit.org/pub/fasthalffloatconversion.pdf
 */
public final class HalfFloat {

    public static final short ZERO = 0;
    public static final short ONE;
    // Integers equal to or above 65520 are rounded to "infinity"
    public static final float MAX_FLOAT_INTEGER = 65520f;
    /** (2-2^-10) * 2^15 */
    public static final float MAX_FLOAT = 65504f;

    /**
     * Smallest positive e for which HalfFloat (1.0 + e) != HalfFloat (1.0)
     */
    public static final float EPSILON = 0.00097656f;

    private static final int[] mantissatable;
    private static final int[] exponenttable;
    private static final short[] offsettable;
    private static final short[] basetable;
    private static final byte[] shifttable;
    static {// lookup tables are 10 KB in total
        mantissatable = new int[2048]; // 8192 bytes
        exponenttable = new int[64]; // 256 bytes
        offsettable = new short[64]; // 128 bytes
        basetable = new short[512]; // 1024 bytes
        shifttable = new byte[512]; // 512 bytes
        populateTableEntries();
        ONE = floatToHalfFloat(1f);
    }

    private HalfFloat() {}

    public static float halfFloatToFloat(final short f16) {
        int i = ((f16 & 0xFFFF) >> 10) & 0xFF;
        int j = (offsettable[i] + (f16 & 0x3FF)) & 0x7FF;
        int bits = mantissatable[j] + exponenttable[i];
        return Float.intBitsToFloat(bits);
    }

    public static short floatToHalfFloat(final float f32) {
        int bits = Float.floatToRawIntBits(f32);
        int i = (bits >> 23) & 0x1FF;
        return (short) (basetable[i] + ((bits & 0x007FFFFF) >> shifttable[i]));
    }

    public static int halfFloatToFloatBits(final short f16) {
        int i = f16 >> 10;
        int j = offsettable[i] + (f16 & 0x3FF);
        return mantissatable[j] + exponenttable[i];
    }

    public static short floatBitsToHalfFloat(final int f32b) {
        int i = (f32b >> 23) & 0x1FF;
        return (short) (basetable[i] + ((f32b & 0x007FFFFF) >> shifttable[i]));
    }

    private static void populateTableEntries() {
        populateMantissaTable(mantissatable);
        populateExponentTable(exponenttable);
        populateOffsetTable(offsettable);

        for (int i = 0; i < 256; i++) {
            final int e = i - 127;
            if (e < -24) { // Very small numbers map to zero
                //basetable[i | 0x000] = (short) 0x0000;
                basetable[i | 0x100] = (short) 0x8000;
                shifttable[i | 0x000] = 24;
                shifttable[i | 0x100] = 24;
            } else if (e < -14) { // Small numbers map to denorms
                basetable[i | 0x000] = (short) (0x0400 >> (-e - 14));
                basetable[i | 0x100] = (short) ((0x0400 >> (-e - 14)) | 0x8000);
                shifttable[i | 0x000] = (byte) (-e - 1);
                shifttable[i | 0x100] = (byte) (-e - 1);
            } else if (e <= 15) { // Normal numbers just lose precision
                basetable[i | 0x000] = (short) ((e + 15) << 10);
                basetable[i | 0x100] = (short) (((e + 15) << 10) | 0x8000);
                shifttable[i | 0x000] = 13;
                shifttable[i | 0x100] = 13;
            } else if (e < 128) { // Large numbers map to Infinity
                basetable[i | 0x000] = (short) 0x7C00;
                basetable[i | 0x100] = (short) 0xFC00;
                shifttable[i | 0x000] = 24;
                shifttable[i | 0x100] = 24;
            } else { // Infinity and NaN's stay Infinity and NaN's
                basetable[i | 0x000] = (short) 0x7C00;
                basetable[i | 0x100] = (short) 0xFC00;
                shifttable[i | 0x000] = 13;
                shifttable[i | 0x100] = 13;
            }
        }
    }

    private static void populateMantissaTable(final int[] mantissatable) {
        mantissatable[0] = 0;
        for (int i = 1; i < 1024; i++) {
            mantissatable[i] = convertMantissa(i);
        }
        for (int i = 1024; i < 2048; i++) {
            mantissatable[i] = 0x38000000 + ((i - 1024) << 13);
        }
    }

    private static int convertMantissa(final int i) {
        int m = i << 13; // Zero pad mantissa bits
        int e = 0; // Zero exponent

        while ((m & 0x00800000) == 0) {// While not normalized
            e -= 0x00800000; // Decrement exponent (1<<23)
            m <<= 1; // Shift mantissa
        }

        m &= ~0x00800000; // Clear leading 1 bit
        e += 0x38800000; // Adjust bias ((127-14)<<23)
        return m | e; // Return combined number
    }

    private static void populateExponentTable(final int[] exponenttable) {
        exponenttable[0] = 0;
        for (int i = 1; i < 31; i++) {
            exponenttable[i] = i << 23;
        }
        exponenttable[31] = 0x47800000;
        exponenttable[32] = 0x80000000;
        for (int i = 33; i < 63; i++) {
            exponenttable[i] = 0x80000000 + ((i - 32) << 23);
        }
        exponenttable[63] = 0xC7800000;
    }

    private static void populateOffsetTable(final short[] offsettable) {
        offsettable[0] = 0;
        for (int i = 1; i < 64; i++) {
            offsettable[i] = 1024;
        }
        offsettable[32] = 0;
    }

    public static boolean isRepresentable(final float f) {
        return Math.abs(f) <= HalfFloat.MAX_FLOAT_INTEGER;
    }

    public static boolean isRepresentable(final float f, final boolean strict) {
        if (strict) {
            return Math.abs(f) <= HalfFloat.MAX_FLOAT;
        } else {
            return Math.abs(f) <= HalfFloat.MAX_FLOAT_INTEGER;
        }
    }

    public static void checkRange(final float f) {
        if (Math.abs(f) > HalfFloat.MAX_FLOAT) {
            throw new IllegalArgumentException("Acceptable maximum weight is "
                    + HalfFloat.MAX_FLOAT + ": " + f);
        }
    }

}
