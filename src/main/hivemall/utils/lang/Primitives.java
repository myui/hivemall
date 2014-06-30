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

    public static char[] toChars(final byte[] b) {
        return toChars(b, 0, b.length);
    }

    public static char[] toChars(final byte[] b, final int off, final int len) {
        final int clen = len >>> 1;
        final char[] c = new char[clen];
        for(int i = 0; i < clen; i++) {
            final int j = off + (i << 1);
            c[i] = (char) ((b[j + 1] & 0xFF) + ((b[j + 0]) << 8));
        }
        return c;
    }

}
