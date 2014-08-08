/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013-2014
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
package hivemall.utils.lang;

/**
 * A utility class to deal with half-precision floating-point.
 * <pre>
 * |sign|       exponent          |                   mantissa                                 |
 * | 31 | 30 29 28 27 26 25 24 23 | 22 21 20 19 18 17 16 15 14 13 12 11 10 9 8 7 6 5 4 3 2 1 0 |
 * |    | 128 64 32 16 8  4  2  1
 * </pre>
 * 
 * @see http://en.wikipedia.org/wiki/Half-precision_floating-point_format
 * @see http://en.wikipedia.org/wiki/Single_precision_floating-point_format 
 * @see http://www.fox-toolkit.org/ftp/fasthalffloatconversion.pdf
 */
public final class HalfFloat {

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
    }

    private HalfFloat() {}

    public static float toFloat(final short h) {
        int i = h >> 10;
        int j = (offsettable[i] + (h & 0x3FF)) & 0x7FF;
        int bits = mantissatable[j] + exponenttable[i];
        return Float.intBitsToFloat(bits);
    }

    public static short toHalfFloat(final float f) {
        int bits = Float.floatToRawIntBits(f);
        int i = (bits >> 23) & 0x1FF;
        return (short) (basetable[i] + ((bits & 0x007FFFFF) >> shifttable[i]));
    }

    private static void populateTableEntries() {
        populateMantissaTable(mantissatable);
        populateExponentTable(exponenttable);
        populateOffsetTable(offsettable);

        for(int i = 0; i < 256; i++) {
            final int e = i - 127;
            if(e < -24) { // Very small numbers map to zero
                basetable[i | 0x000] = (short) 0x0000;
                basetable[i | 0x100] = (short) 0x8000;
                shifttable[i | 0x000] = 24;
                shifttable[i | 0x100] = 24;
            } else if(e < -14) { // Small numbers map to denorms
                basetable[i | 0x000] = (short) (0x0400 >> (-e - 14));
                basetable[i | 0x100] = (short) ((0x0400 >> (-e - 14)) | 0x8000);
                shifttable[i | 0x000] = (byte) (-e - 1);
                shifttable[i | 0x100] = (byte) (-e - 1);
            } else if(e <= 15) { // Normal numbers just lose precision
                basetable[i | 0x000] = (short) ((e + 15) << 10);
                basetable[i | 0x100] = (short) (((e + 15) << 10) | 0x8000);
                shifttable[i | 0x000] = 13;
                shifttable[i | 0x100] = 13;
            } else if(e < 128) { // Large numbers map to Infinity
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
        for(int i = 1; i < 1024; i++) {
            mantissatable[i] = convertMantissa(i);
        }
        for(int i = 1024; i < 2048; i++) {
            mantissatable[i] = 0x38000000;
        }
    }

    private static int convertMantissa(final int i) {
        int m = i << 13; // Zero pad mantissa bits
        int e = 0; // Zero exponent

        while((m & 0x00800000) == 0) {// While not normalized
            e -= 0x00800000; // Decrement exponent (1<<23)
            m <<= 1; // Shift mantissa
        }

        m &= ~0x00800000; // Clear leading 1 bit
        e += 0x38800000; // Adjust bias ((127-14)<<23)
        return m | e; // Return combined number
    }

    private static void populateExponentTable(final int[] exponenttable) {
        exponenttable[0] = 0;
        for(int i = 1; i < 31; i++) {
            exponenttable[i] = i << 23;
        }
        exponenttable[31] = 0x47800000;
        exponenttable[32] = 0x80000000;
        for(int i = 33; i < 63; i++) {
            exponenttable[i] = 0x80000000 + ((i - 32) << 23);
        }
        exponenttable[63] = 0xC7800000;
    }

    private static void populateOffsetTable(final short[] offsettable) {
        offsettable[0] = 0;
        for(int i = 1; i < 64; i++) {
            offsettable[i] = 1024;
        }
        offsettable[32] = 0;
    }

}
