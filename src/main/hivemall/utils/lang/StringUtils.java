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

public final class StringUtils {

    private StringUtils() {}

    public static byte[] getBytes(final String s) {
        final int len = s.length();
        final byte[] b = new byte[len * 2];
        for(int i = 0; i < len; i++) {
            Primitives.putChar(b, i * 2, s.charAt(i));
        }
        return b;
    }

    public static String toString(byte[] b) {
        return toString(b, 0, b.length);
    }

    public static String toString(byte[] b, int off, int len) {
        final int clen = len >>> 1;
        final char[] c = new char[clen];
        for(int i = 0; i < clen; i++) {
            final int j = off + (i << 1);
            c[i] = (char) ((b[j + 1] & 0xFF) + ((b[j + 0]) << 8));
        }
        return new String(c);
    }

}
