/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013
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
package hivemall.ftvec.hashing;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

public class MurmurHash3UDF extends UDF {

    public static final int DEFAULT_NUM_FEATURES = 16777216;

    public int evaluate(String word) throws UDFArgumentException {
        return evaluate(word, DEFAULT_NUM_FEATURES);
    }

    public int evaluate(String word, boolean rawValue) throws UDFArgumentException {
        if(rawValue) {
            if(word == null) {
                throw new UDFArgumentException("argument must not be null");
            }
            return murmurhash3_x86_32(word, 0, word.length(), 0x9747b28c);
        } else {
            return evaluate(word, DEFAULT_NUM_FEATURES);
        }
    }

    public int evaluate(String word, int numFeatures) throws UDFArgumentException {
        if(word == null) {
            throw new UDFArgumentException("argument must not be null");
        }
        int r = murmurhash3_x86_32(word, 0, word.length(), 0x9747b28c) % numFeatures;
        if(r < 0) {
            r += numFeatures;
        }
        return r;
    }

    public int evaluate(String... words) throws UDFArgumentException {
        if(words == null) {
            throw new UDFArgumentException("argument must not be null");
        }
        if(words.length == 0) {
            return 0;
        }
        final StringBuilder b = new StringBuilder();
        b.append(words[0]);
        for(int i = 1; i < words.length; i++) {
            b.append('\t');
            b.append(words[i]);
        }
        String s = b.toString();
        return evaluate(s);
    }

    public int evaluate(String[] words, int numFeatures) throws UDFArgumentException {
        if(words == null) {
            throw new UDFArgumentException("argument must not be null");
        }
        if(words.length == 0) {
            return 0;
        }
        final StringBuilder b = new StringBuilder();
        b.append(words[0]);
        for(int i = 1; i < words.length; i++) {
            b.append('\t');
            b.append(words[i]);
        }
        String s = b.toString();
        return evaluate(s, numFeatures);
    }

    /**
     * @return hash value of range from 0 to 2^24 (16777216).
     */
    public static int murmurhash3(final String data) {
        return murmurhash3(data, DEFAULT_NUM_FEATURES);
    }

    public static int murmurhash3(final String data, final int numFeatures) {
        int r = murmurhash3_x86_32(data, 0, data.length(), 0x9747b28c) % numFeatures;
        if(r < 0) {
            r += numFeatures;
        }
        return r;
    }

    /** Returns the MurmurHash3_x86_32 hash. */
    public static int murmurhash3_x86_32(final CharSequence data, final int offset, final int len, final int seed) {
        final int c1 = 0xcc9e2d51;
        final int c2 = 0x1b873593;

        int h1 = seed;

        int pos = offset;
        int end = offset + len;
        int k1 = 0;
        int k2 = 0;
        int shift = 0;
        int bits = 0;
        int nBytes = 0; // length in UTF8 bytes

        while(pos < end) {
            int code = data.charAt(pos++);
            if(code < 0x80) {
                k2 = code;
                bits = 8;
            } else if(code < 0x800) {
                k2 = (0xC0 | (code >> 6)) | ((0x80 | (code & 0x3F)) << 8);
                bits = 16;
            } else if(code < 0xD800 || code > 0xDFFF || pos >= end) {
                // we check for pos>=end to encode an unpaired surrogate as 3 bytes.
                k2 = (0xE0 | (code >> 12)) | ((0x80 | ((code >> 6) & 0x3F)) << 8)
                        | ((0x80 | (code & 0x3F)) << 16);
                bits = 24;
            } else {
                // surrogate pair
                // int utf32 = pos < end ? (int) data.charAt(pos++) : 0;
                int utf32 = (int) data.charAt(pos++);
                utf32 = ((code - 0xD7C0) << 10) + (utf32 & 0x3FF);
                k2 = (0xff & (0xF0 | (utf32 >> 18))) | ((0x80 | ((utf32 >> 12) & 0x3F))) << 8
                        | ((0x80 | ((utf32 >> 6) & 0x3F))) << 16 | (0x80 | (utf32 & 0x3F)) << 24;
                bits = 32;
            }

            k1 |= k2 << shift;

            shift += bits;
            if(shift >= 32) {
                // mix after we have a complete word

                k1 *= c1;
                k1 = (k1 << 15) | (k1 >>> 17); // ROTL32(k1,15);
                k1 *= c2;

                h1 ^= k1;
                h1 = (h1 << 13) | (h1 >>> 19); // ROTL32(h1,13);
                h1 = h1 * 5 + 0xe6546b64;

                shift -= 32;
                // unfortunately, java won't let you shift 32 bits off, so we need to check for 0
                if(shift != 0) {
                    k1 = k2 >>> (bits - shift); // bits used == bits - newshift
                } else {
                    k1 = 0;
                }
                nBytes += 4;
            }

        } // inner

        // handle tail
        if(shift > 0) {
            nBytes += shift >> 3;
            k1 *= c1;
            k1 = (k1 << 15) | (k1 >>> 17); // ROTL32(k1,15);
            k1 *= c2;
            h1 ^= k1;
        }

        // finalization
        h1 ^= nBytes;

        // fmix(h1);
        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;

        return h1;
    }
}
