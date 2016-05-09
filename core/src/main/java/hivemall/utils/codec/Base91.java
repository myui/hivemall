/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
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
/*
 * basE91 encoding/decoding routines
 *
 * Copyright (c) 2000-2006 Joachim Henke
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  - Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  - Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *  - Neither the name of Joachim Henke nor the names of his contributors may
 *    be used to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package hivemall.utils.codec;

import hivemall.utils.io.FastByteArrayOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.annotation.Nonnull;

/**
 * BASE91 is an advanced method for encoding binary data as ASCII, i.e., UTF-8 1 byte, characters.
 * 
 * It uses ACSII printable characters expect Backslash (0x5C), Single Quote (0x27), and Double Quote
 * (0x22) while the original version of basE91 use Double Quote (0x22) instead of Dash (0x2D) for
 * the encoded string.
 */
public final class Base91 {

    public static final float WORST_ENCODING_RATIO = 1.2308f;
    public static final float BEST_ENCODING_RATIO = 1.1429f;

    private static final byte[] ENCODING_TABLE;
    private static final byte[] DECODING_TABLE;
    private static final int BASE;

    static {
        String ts = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
                + "!#$%&()*+,-./:;<=>?@[]^_`{|}~";
        ENCODING_TABLE = ts.getBytes();
        BASE = ENCODING_TABLE.length;

        DECODING_TABLE = new byte[256];
        for (int i = 0; i < 256; i++) {
            DECODING_TABLE[i] = -1;
        }
        for (int i = 0; i < BASE; i++) {
            DECODING_TABLE[ENCODING_TABLE[i]] = (byte) i;
        }
    }

    @Nonnull
    public static byte[] encode(@Nonnull final byte[] input) {
        return encode(input, 0, input.length);
    }

    @Nonnull
    public static byte[] encode(@Nonnull final byte[] input, final int offset, final int len) {
        int estimatedSize = (int) Math.ceil(len * WORST_ENCODING_RATIO);
        final FastByteArrayOutputStream output = new FastByteArrayOutputStream(estimatedSize);
        try {
            encode(input, offset, len, output);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to convert BINARY to BASE91 string", e);
        }
        return output.toByteArray();
    }

    public static void encode(@Nonnull final byte[] input, @Nonnull final OutputStream output)
            throws IOException {
        encode(input, 0, input.length, output);
    }

    public static void encode(@Nonnull final byte[] input, final int offset, final int len,
            @Nonnull final OutputStream output) throws IOException {
        int ebq = 0;
        int en = 0;
        for (int i = offset; i < len; ++i) {
            ebq |= (input[i] & 255) << en;
            en += 8;
            if (en > 13) {
                int ev = ebq & 0x1FFF; // 0001 1111 1111 1111
                if (ev > 88) {
                    ebq >>= 13;
                    en -= 13;
                } else {
                    ev = ebq & 0x3FFF; // 0011 1111 1111 1111
                    ebq >>= 14;
                    en -= 14;
                }
                output.write(ENCODING_TABLE[ev % BASE]);
                output.write(ENCODING_TABLE[ev / BASE]);
            }
        }
        if (en > 0) {
            output.write(ENCODING_TABLE[ebq % BASE]);
            if (en > 7 || ebq > 90) {
                output.write(ENCODING_TABLE[ebq / BASE]);
            }
        }
    }

    public static void encode(@Nonnull final InputStream in, @Nonnull final OutputStream out)
            throws IOException {
        int ebq = 0;
        int en = 0;

        int b;
        while ((b = in.read()) != -1) {
            ebq |= (b & 255) << en;
            en += 8;
            if (en > 13) {
                int ev = ebq & 0x1FFF; // 0001 1111 1111 1111
                if (ev > 88) {
                    ebq >>= 13;
                    en -= 13;
                } else {
                    ev = ebq & 0x3FFF; // 0011 1111 1111 1111
                    ebq >>= 14;
                    en -= 14;
                }
                out.write(ENCODING_TABLE[ev % BASE]);
                out.write(ENCODING_TABLE[ev / BASE]);
            }
        }
        if (en > 0) {
            out.write(ENCODING_TABLE[ebq % BASE]);
            if (en > 7 || ebq > 90) {
                out.write(ENCODING_TABLE[ebq / BASE]);
            }
        }
    }

    @Nonnull
    public static byte[] decode(@Nonnull final byte[] input) {
        return decode(input, 0, input.length);
    }

    @Nonnull
    public static byte[] decode(@Nonnull final byte[] input, final int offset, final int len) {
        int expectedSize = Math.round(len / BEST_ENCODING_RATIO);
        final FastByteArrayOutputStream output = new FastByteArrayOutputStream(expectedSize);
        try {
            decode(input, offset, len, output);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to decode BASE91 binary", e);
        }
        return output.toByteArray();
    }

    public static void decode(@Nonnull final byte[] input, @Nonnull final OutputStream output)
            throws IOException {
        decode(input, 0, input.length, output);
    }

    public static void decode(@Nonnull final byte[] input, final int offset, final int len,
            @Nonnull final OutputStream output) throws IOException {
        int dbq = 0;
        int dn = 0;
        int dv = -1;
        for (int i = offset; i < len; ++i) {
            if (DECODING_TABLE[input[i]] == -1) {
                continue;
            }
            if (dv == -1) {
                dv = DECODING_TABLE[input[i]];
            } else {
                dv += DECODING_TABLE[input[i]] * BASE;
                dbq |= dv << dn;
                dn += (dv & 0x1FFF) > 88 ? 13 : 14;
                do {
                    output.write((byte) dbq);
                    dbq >>= 8;
                    dn -= 8;
                } while (dn >= 8);
                dv = -1;
            }
        }
        if (dv != -1) {
            output.write((byte) (dbq | dv << dn));
        }
    }

    public static void decode(@Nonnull final InputStream in, @Nonnull final OutputStream out)
            throws IOException {
        int dbq = 0;
        int dn = 0;
        int dv = -1;

        int b;
        while ((b = in.read()) != -1) {
            if (DECODING_TABLE[b] == -1) {
                continue;
            }
            if (dv == -1) {
                dv = DECODING_TABLE[b];
            } else {
                dv += DECODING_TABLE[b] * BASE;
                dbq |= dv << dn;
                dn += (dv & 0x1FFF) > 88 ? 13 : 14;
                do {
                    out.write((byte) dbq);
                    dbq >>= 8;
                    dn -= 8;
                } while (dn >= 8);
                dv = -1;
            }
        }
        if (dv != -1) {
            out.write((byte) (dbq | dv << dn));
        }
    }

}
