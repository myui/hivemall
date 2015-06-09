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

    /**
     * Checks whether the String a valid Java number. this code is ported from
     * jakarta commons lang.
     * 
     * @link 
     *       http://jakarta.apache.org/commons/lang/apidocs/org/apache/commons/lang
     *       /math/NumberUtils.html
     */
    public static boolean isNumber(final String str) {
        if(str == null || str.length() == 0) {
            return false;
        }
        char[] chars = str.toCharArray();
        int sz = chars.length;
        boolean hasExp = false;
        boolean hasDecPoint = false;
        boolean allowSigns = false;
        boolean foundDigit = false;
        // deal with any possible sign up front
        int start = (chars[0] == '-') ? 1 : 0;
        if(sz > start + 1) {
            if(chars[start] == '0' && chars[start + 1] == 'x') {
                int i = start + 2;
                if(i == sz) {
                    return false; // str == "0x"
                }
                // checking hex (it can't be anything else)
                for(; i < chars.length; i++) {
                    if((chars[i] < '0' || chars[i] > '9') && (chars[i] < 'a' || chars[i] > 'f')
                            && (chars[i] < 'A' || chars[i] > 'F')) {
                        return false;
                    }
                }
                return true;
            }
        }
        sz--; // don't want to loop to the last char, check it afterwords
        // for type qualifiers
        int i = start;
        // loop to the next to last char or to the last char if we need another digit to
        // make a valid number (e.g. chars[0..5] = "1234E")
        while(i < sz || (i < sz + 1 && allowSigns && !foundDigit)) {
            if(chars[i] >= '0' && chars[i] <= '9') {
                foundDigit = true;
                allowSigns = false;

            } else if(chars[i] == '.') {
                if(hasDecPoint || hasExp) {
                    // two decimal points or dec in exponent   
                    return false;
                }
                hasDecPoint = true;
            } else if(chars[i] == 'e' || chars[i] == 'E') {
                // we've already taken care of hex.
                if(hasExp) {
                    // two E's
                    return false;
                }
                if(!foundDigit) {
                    return false;
                }
                hasExp = true;
                allowSigns = true;
            } else if(chars[i] == '+' || chars[i] == '-') {
                if(!allowSigns) {
                    return false;
                }
                allowSigns = false;
                foundDigit = false; // we need a digit after the E
            } else {
                return false;
            }
            i++;
        }
        if(i < chars.length) {
            if(chars[i] >= '0' && chars[i] <= '9') {
                // no type qualifier, OK
                return true;
            }
            if(chars[i] == 'e' || chars[i] == 'E') {
                // can't have an E at the last byte
                return false;
            }
            if(!allowSigns
                    && (chars[i] == 'd' || chars[i] == 'D' || chars[i] == 'f' || chars[i] == 'F')) {
                return foundDigit;
            }
            if(chars[i] == 'l' || chars[i] == 'L') {
                // not allowing L with an exponent
                return foundDigit && !hasExp;
            }
            // last character is illegal
            return false;
        }
        // allowSigns is true iff the val ends in 'E'
        // found digit it to make sure weird stuff like '.' and '1E-' doesn't pass
        return !allowSigns && foundDigit;
    }

}
