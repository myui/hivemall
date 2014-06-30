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

import java.util.BitSet;

public final class BitUtils {

    public static BitSet toBitSet(final String s) {
        final int len = s.length();
        final BitSet result = new BitSet(len);
        for(int i = 0; i < len; i++) {
            if(s.charAt(i) == '1') {
                result.set(len - i - 1);
            }
        }
        return result;
    }

    public static String toBinaryString(final BitSet bits) {
        final int len = bits.length();
        final char[] data = new char[len];
        for(int i = 0; i < len; i++) {
            data[len - i - 1] = bits.get(i) ? '1' : '0';
        }
        return String.valueOf(data);
    }

}
