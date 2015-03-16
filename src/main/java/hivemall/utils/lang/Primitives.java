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
