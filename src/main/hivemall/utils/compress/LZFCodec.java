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
package hivemall.utils.compress;

import hivemall.utils.lang.Primitives;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 * @link http://www.goof.com/pcg/marc/liblzf.html
 */
public final class LZFCodec implements CompressionCodec {

    private static final int HASH_SIZE = (1 << 14);
    private static final int MAX_LITERAL = (1 << 5);
    private static final int MAX_OFF = (1 << 13);
    private static final int MAX_REF = ((1 << 8) + (1 << 3));

    public LZFCodec() {}

    public byte[] compress(final byte[] in) {
        return compress(in, in.length);
    }

    public byte[] compress(final byte[] in, final int inLength) {
        final byte[] out = new byte[inLength * 3 >> 1];
        final int size = compress(in, inLength, out, 0);
        final byte[] dst = new byte[size + 3];
        dst[0] = (byte) inLength;
        dst[1] = (byte) (inLength >> 8);
        dst[2] = (byte) (inLength >> 16);
        System.arraycopy(out, 0, dst, 3, size);
        return dst;
    }

    public static int compress(final byte[] in, final int inLen, final byte[] out, int outPos) {
        int inPos = 0;
        final int[] hashTab = new int[HASH_SIZE];
        int literals = 0;
        int hval = first(in, inPos);
        while(true) {
            if(inPos < inLen - 4) {
                hval = next(hval, in, inPos);
                int off = hash(hval);
                int ref = hashTab[off];
                hashTab[off] = inPos;
                off = inPos - ref - 1;
                if(off < MAX_OFF && ref > 0 && in[ref + 2] == in[inPos + 2]
                        && in[ref + 1] == in[inPos + 1] && in[ref] == in[inPos]) {
                    int maxlen = inLen - inPos - 2;
                    maxlen = maxlen > MAX_REF ? MAX_REF : maxlen;
                    int len = 3;
                    while(len < maxlen && in[ref + len] == in[inPos + len]) {
                        len++;
                    }
                    len -= 2;
                    if(literals != 0) {
                        out[outPos++] = (byte) (literals - 1);
                        literals = -literals;
                        do {
                            out[outPos++] = in[inPos + literals++];
                        } while(literals != 0);
                    }
                    if(len < 7) {
                        out[outPos++] = (byte) ((off >> 8) + (len << 5));
                    } else {
                        out[outPos++] = (byte) ((off >> 8) + (7 << 5));
                        out[outPos++] = (byte) (len - 7);
                    }
                    out[outPos++] = (byte) off;
                    inPos += len;
                    hval = first(in, inPos);
                    hval = next(hval, in, inPos);
                    hashTab[hash(hval)] = inPos++;
                    hval = next(hval, in, inPos);
                    hashTab[hash(hval)] = inPos++;
                    continue;
                }
            } else if(inPos == inLen) {
                break;
            }
            inPos++;
            literals++;
            if(literals == MAX_LITERAL) {
                out[outPos++] = (byte) (literals - 1);
                literals = -literals;
                do {
                    out[outPos++] = in[inPos + literals++];
                } while(literals != 0);
            }
        }
        if(literals != 0) {
            out[outPos++] = (byte) (literals - 1);
            for(int i = inPos - literals; i != inPos; i++) {
                out[outPos++] = in[i];
            }
        }
        return outPos;
    }

    public byte[] decompress(final byte[] in) {
        final int size = ((in[2] & 0xff) << 16) + ((in[1] & 0xff) << 8) + (in[0] & 0xff);
        final byte[] dst = new byte[size];
        decompress(in, 3, dst, 0, size);
        return dst;
    }

    public char[] decompressAsChars(final byte[] in) {
        final int size = ((in[2] & 0xff) << 16) + ((in[1] & 0xff) << 8) + (in[0] & 0xff);
        final byte[] dst = new byte[size];
        decompress(in, 3, dst, 0, size);
        return Primitives.toChars(dst, 0, size);
    }

    public static void decompress(final byte[] in, int inPos, final byte[] out, int outPos, final int outLength) {
        do {
            final int ctrl = in[inPos++] & 255;
            if(ctrl < MAX_LITERAL) {
                // literal run
                for(int i = 0; i <= ctrl; i++) {
                    out[outPos++] = in[inPos++];
                }
            } else {
                // back reference
                int len = ctrl >> 5;
                int ref = outPos - ((ctrl & 0x1f) << 8) - 1;
                if(len == 7) {
                    len += in[inPos++] & 255;
                }
                ref -= in[inPos++] & 255;
                len += outPos + 2;
                out[outPos++] = out[ref++];
                out[outPos++] = out[ref++];
                while(outPos < len) {
                    out[outPos++] = out[ref++];
                }
            }
        } while(outPos < outLength);
    }

    private static int first(final byte[] in, final int inPos) {
        return (in[inPos] << 8) + (in[inPos + 1] & 255);
    }

    private static int next(final int v, final byte[] in, final int inPos) {
        return (v << 8) + (in[inPos + 2] & 255);
    }

    private static int hash(final int h) {
        return ((h * 184117) >> 9) & (HASH_SIZE - 1);
    }

}