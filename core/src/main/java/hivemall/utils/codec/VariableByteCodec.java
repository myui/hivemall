/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package hivemall.utils.codec;

import hivemall.utils.io.FastByteArrayInputStream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * First 1 bit of each byte is flag to judge whether more lookahead is required or not.
 * 
 * @link http://www.cs.otago.ac.nz/cosc463/2005/compress.htm
 */
public final class VariableByteCodec {

    public VariableByteCodec() {}

    public static int requiredBytes(long val) {
        int i = 1;
        while (val > 0x7FL) {
            val >>= 7;
            i++;
        }
        return i;
    }

    public static int requiredBytes(int val) {
        int i = 1;
        while (val > 0x7F) {
            val >>= 7;
            i++;
        }
        return i;
    }

    public static byte[] encodeUnsignedLong(long val) {
        if (val < 0) {
            throw new IllegalArgumentException("Illegal value: " + val);
        }
        final byte[] buf = new byte[9];
        int i = 0;
        while (val > 0x7FL) {
            buf[i++] = (byte) ((val & 0x7F) | 0x80);
            val >>= 7;
        }
        buf[i++] = (byte) val;
        final byte[] rbuf = new byte[i];
        System.arraycopy(buf, 0, rbuf, 0, i);
        return rbuf;
    }

    public static void encodeUnsignedLong(long val, final byte[] out, int offset) {
        if (val < 0) {
            throw new IllegalArgumentException("Illegal value: " + val);
        }
        while (val > 0x7FL) {
            out[offset++] = (byte) ((val & 0x7F) | 0x80);
            val >>= 7;
        }
        out[offset++] = (byte) val;
    }

    public static void encodeUnsignedLong(long val, final OutputStream os) throws IOException {
        if (val < 0) {
            throw new IllegalArgumentException("Illegal value: " + val);
        }
        while (val > 0x7FL) {
            final byte b = (byte) ((val & 0x7F) | 0x80);
            os.write(b);
            val >>= 7;
        }
        final byte b = (byte) val;
        os.write(b);
    }

    public static void encodeUnsignedLong(long val, final DataOutput out) throws IOException {
        if (val < 0) {
            throw new IllegalArgumentException("Illegal value: " + val);
        }
        while (val > 0x7FL) {
            final byte b = (byte) ((val & 0x7F) | 0x80);
            out.write(b);
            val >>= 7;
        }
        final byte b = (byte) val;
        out.write(b);
    }

    public static long decodeUnsignedLong(final byte[] val) {
        return decodeUnsignedLong(val, 0);
    }

    public static long decodeUnsignedLong(final byte[] val, final int from) {
        long x = 0;
        long b = 0;
        int shift = 0;

        final int vlen = val.length;
        for (int i = from; i < vlen; i++) {
            b = val[i];
            final long more = b & 0x80L;
            x |= (b & 0x7FL) << shift;
            if (more != 0x80L) {
                break;
            }
            shift += 7;
        }
        return x;
    }

    public static long decodeUnsignedLong(final InputStream is) throws IOException {
        long x = 0;
        long b = 0;
        int shift = 0;

        while (true) {
            b = is.read();
            x |= (b & 0x7FL) << shift;
            final long more = b & 0x80L;
            if (more != 0x80L) {
                break;
            }
            shift += 7;
        }
        return x;
    }

    public static long decodeUnsignedLong(final DataInput in) throws IOException {
        long x = 0;
        long b = 0;
        int shift = 0;

        while (true) {
            b = in.readByte();
            x |= (b & 0x7FL) << shift;
            final long more = b & 0x80L;
            if (more != 0x80L) {
                break;
            }
            shift += 7;
        }
        return x;
    }

    public static void encodeUnsignedInt(int val, final OutputStream os) throws IOException {
        if (val < 0) {
            throw new IllegalArgumentException("Illegal value: " + val);
        }
        while (val > 0x7F) {
            final byte b = (byte) ((val & 0x7F) | 0x80);
            os.write(b);
            val >>= 7;
        }
        final byte b = (byte) val;
        os.write(b);
    }

    public static void encodeUnsignedInt(int val, final DataOutput out) throws IOException {
        if (val < 0) {
            throw new IllegalArgumentException("Illegal value: " + val);
        }
        while (val > 0x7F) {
            final byte b = (byte) ((val & 0x7F) | 0x80);
            out.write(b);
            val >>= 7;
        }
        final byte b = (byte) val;
        out.write(b);
    }

    public static int decodeUnsignedInt(final byte[] val) {
        return decodeUnsignedInt(val, 0);
    }

    public static int decodeUnsignedInt(final byte[] val, final int from) {
        int x = 0;
        int b = 0;
        int shift = 0;

        final int vlen = val.length;
        for (int i = from; i < vlen; i++) {
            b = val[i];
            x |= (b & 0x7F) << shift;
            if ((b & 0x80) != 0x80) {
                break;
            }
            shift += 7;
        }
        return x;
    }

    public static int decodeUnsignedInt(final InputStream is) throws IOException {
        int x = 0;
        int b = 0;
        int shift = 0;

        while (true) {
            b = is.read();
            x |= (b & 0x7F) << shift;
            if ((b & 0x80) != 0x80) {
                break;
            }
            shift += 7;
        }
        return x;
    }

    public static int decodeUnsignedInt(final DataInput in) throws IOException {
        int x = 0;
        int b = 0;
        int shift = 0;

        while (true) {
            b = in.readByte();
            x |= (b & 0x7F) << shift;
            if ((b & 0x80) != 0x80) {
                break;
            }
            shift += 7;
        }
        return x;
    }

    public static int decodeUnsignedInt(final InputStream is, int b) throws IOException {
        int x = 0;
        int shift = 0;

        while (true) {
            x |= (b & 0x7F) << shift;
            if ((b & 0x80) != 0x80) {
                break;
            }
            shift += 7;
            b = is.read();
        }
        return x;
    }

    public static int decodeUnsignedInt(final FastByteArrayInputStream is) {
        int x = 0;
        int b = 0;
        int shift = 0;

        while (true) {
            b = is.read();
            x |= (b & 0x7F) << shift;
            if ((b & 0x80) != 0x80) {
                break;
            }
            shift += 7;
        }
        return x;
    }

}
