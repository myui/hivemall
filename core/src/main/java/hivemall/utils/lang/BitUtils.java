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

import java.util.BitSet;

import javax.annotation.Nonnull;

public final class BitUtils {

    private BitUtils() {}

    public static BitSet toBitSet(final String s) {
        final int len = s.length();
        final BitSet result = new BitSet(len);
        for (int i = 0; i < len; i++) {
            if (s.charAt(i) == '1') {
                result.set(len - i - 1);
            }
        }
        return result;
    }

    public static String toBinaryString(final BitSet bits) {
        final int len = bits.length();
        final char[] data = new char[len];
        for (int i = 0; i < len; i++) {
            data[len - i - 1] = bits.get(i) ? '1' : '0';
        }
        return String.valueOf(data);
    }

    /**
     * @param nth index starting from 0
     * @return index of n-th set bit or -1 if not found
     */
    public static int indexOfSetBit(@Nonnull final BitSet bits, final int nth) {
        if (nth < 0) {
            throw new IllegalArgumentException("Invalid nth: " + nth);
        }

        int pos = bits.nextSetBit(0);
        for (int i = 0; pos >= 0; pos = bits.nextSetBit(pos + 1), i++) {
            if (i == nth) {
                break;
            }
        }
        return pos;
    }

    public static int indexOfClearBit(@Nonnull final BitSet bits, final int nth, final int lastIndex) {
        int j = bits.nextClearBit(0);
        for (int c = 0; j <= lastIndex; j = bits.nextClearBit(j + 1), c++) {
            if (c == nth) {
                break;
            }
        }
        return j;
    }

}
