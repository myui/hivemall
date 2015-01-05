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

public final class Primitives {

    private Primitives() {}

    public static int toUnsignedShort(final short v) {
        return v & 0xFFFF; // convert to range 0-65535 from -32768-32767.
    }

    public static short parseShort(final String s, final short defaultValue) {
        if(s == null) {
            return defaultValue;
        }
        return Short.parseShort(s);
    }

    public static int parseInt(final String s, final int defaultValue) {
        if(s == null) {
            return defaultValue;
        }
        return Integer.parseInt(s);
    }

    public static long parseLong(final String s, final long defaultValue) {
        if(s == null) {
            return defaultValue;
        }
        return Long.parseLong(s);
    }

    public static float parseFloat(final String s, final float defaultValue) {
        if(s == null) {
            return defaultValue;
        }
        return Float.parseFloat(s);
    }

    public static double parseDouble(final String s, final double defaultValue) {
        if(s == null) {
            return defaultValue;
        }
        return Double.parseDouble(s);
    }

    public static boolean parseBoolean(final String s, final boolean defaultValue) {
        if(s == null) {
            return defaultValue;
        }
        return Boolean.parseBoolean(s);
    }

    public static int compare(final int x, final int y) {
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

    public static void putChar(final byte[] b, final int off, final char val) {
        b[off + 1] = (byte) (val >>> 0);
        b[off] = (byte) (val >>> 8);
    }

}
