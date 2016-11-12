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
package hivemall.utils.io;

import hivemall.utils.codec.ZigZagLEB128Codec;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class IOUtils {
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 4;

    private IOUtils() {}

    public static void closeQuietly(final Closeable channel) {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException e) {
                ;
            }
        }
    }

    public static void closeQuietly(final Closeable... channels) {
        for (Closeable c : channels) {
            if (c != null) {
                try {
                    c.close();
                } catch (IOException e) {
                    ;
                }
            }
        }
    }

    /**
     * Serialize given InputStream as String.
     */
    public static String toString(@Nonnull final InputStream input) throws IOException {
        FastMultiByteArrayOutputStream output = new FastMultiByteArrayOutputStream();
        copy(input, output);
        return output.toString();
    }

    /**
     * Serialize given InputStream as String.
     */
    public static String toString(@Nonnull final InputStream input, final int bufSize)
            throws IOException {
        FastByteArrayOutputStream output = new FastByteArrayOutputStream(bufSize);
        copy(input, output);
        return output.toString();
    }

    @Nonnull
    public static byte[] toByteArray(@Nonnull final InputStream input) throws IOException {
        FastByteArrayOutputStream output = new FastByteArrayOutputStream();
        copy(input, output);
        return output.toByteArray();
    }

    /**
     * InputStream -> OutputStream
     */
    public static int copy(@Nonnull final InputStream input, @Nonnull final OutputStream output)
            throws IOException {
        final byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
        int count = 0;
        int n = 0;
        while (-1 != (n = input.read(buffer))) {
            output.write(buffer, 0, n);
            count += n;
        }
        return count;
    }

    @Nonnull
    public static BufferedReader bufferedReader(@Nonnull InputStream is) {
        InputStreamReader in = new InputStreamReader(is);
        return new BufferedReader(in);
    }

    public static void writeInt(final int v, final OutputStream out) throws IOException {
        out.write((v >>> 24) & 0xFF);
        out.write((v >>> 16) & 0xFF);
        out.write((v >>> 8) & 0xFF);
        out.write((v >>> 0) & 0xFF);
    }

    /**
     * @return may be negative value when EOF is detected.
     */
    public static int readInt(final InputStream in) throws IOException {
        final int ch1 = in.read();
        final int ch2 = in.read();
        final int ch3 = in.read();
        final int ch4 = in.read();
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }

    public static void writeChar(final char v, final OutputStream out) throws IOException {
        out.write(0xff & (v >> 8));
        out.write(0xff & v);
    }

    public static void writeChar(final char v, final FastByteArrayOutputStream out) {
        out.write(0xff & (v >> 8));
        out.write(0xff & v);
    }

    public static char readChar(final InputStream in) throws IOException {
        final int a = in.read();
        final int b = in.read();
        return (char) ((a << 8) | (b & 0xff));
    }


    public static void writeBytes(@Nonnull final byte[] src, @Nonnull final OutputStream dst)
            throws IOException {
        if (src != null) {
            dst.write(src);
        }
    }

    public static void writeBytes(@Nonnull final byte[] src, final int off, final int len,
            @Nonnull final OutputStream dst) throws IOException {
        if (src != null) {
            dst.write(src, off, len);
        }
    }

    public static void writeString(@Nullable final String s, final ObjectOutputStream out)
            throws IOException {
        writeString(s, (DataOutput) out);
    }

    public static void writeString(@Nullable final String s, final DataOutputStream out)
            throws IOException {
        writeString(s, (DataOutput) out);
    }

    public static void writeString(@Nullable final String s, final DataOutput out)
            throws IOException {
        if (s == null) {
            out.writeInt(-1);
            return;
        }
        final int len = s.length();
        out.writeInt(len);
        for (int i = 0; i < len; i++) {
            int v = s.charAt(i);
            out.writeChar(v);
        }
    }

    public static void writeString(@Nullable final String s, final OutputStream out)
            throws IOException {
        if (s == null) {
            writeInt(-1, out);
            return;
        }
        final int len = s.length();
        writeInt(len, out);
        for (int i = 0; i < len; i++) {
            char c = s.charAt(i);
            writeChar(c, out);
        }
    }

    @Nullable
    public static String readString(@Nonnull final ObjectInputStream in) throws IOException {
        return readString((DataInput) in);
    }

    @Nullable
    public static String readString(@Nonnull final DataInputStream in) throws IOException {
        return readString((DataInput) in);
    }

    @Nullable
    public static String readString(@Nonnull final DataInput in) throws IOException {
        final int len = in.readInt();
        if (len == -1) {
            return null;
        }
        final char[] ch = new char[len];
        for (int i = 0; i < len; i++) {
            ch[i] = in.readChar();
        }
        return new String(ch);
    }

    @Nullable
    public static String readString(@Nonnull final InputStream in) throws IOException {
        final int len = readInt(in);
        if (len == -1) {
            return null;
        }
        final char[] ch = new char[len];
        for (int i = 0; i < len; i++) {
            ch[i] = readChar(in);
        }
        return new String(ch);
    }

    public static void writeFloats(@Nonnull final float[] floats, @Nonnull final DataOutput out)
            throws IOException {
        final int size = floats.length;
        out.writeInt(size);
        for (int i = 0; i < size; i++) {
            out.writeFloat(floats[i]);
        }
    }

    public static void writeFloats(@Nonnull final float[] floats, final int size,
            @Nonnull final DataOutput out) throws IOException {
        for (int i = 0; i < size; i++) {
            out.writeFloat(floats[i]);
        }
    }

    @Nonnull
    public static float[] readFloats(@Nonnull final DataInput in) throws IOException {
        final int size = in.readInt();
        final float[] floats = new float[size];
        for (int i = 0; i < size; i++) {
            floats[i] = in.readFloat();
        }
        return floats;
    }

    @Nonnull
    public static float[] readFloats(@Nonnull final DataInput in, final int size)
            throws IOException {
        final float[] floats = new float[size];
        for (int i = 0; i < size; i++) {
            floats[i] = in.readFloat();
        }
        return floats;
    }

    @Nonnull
    public static void readFloats(@Nonnull final DataInput in, @Nonnull final float[] dst)
            throws IOException {
        for (int i = 0, len = dst.length; i < len; i++) {
            dst[i] = in.readFloat();
        }
    }

    @Deprecated
    public static void writeVFloats(@Nonnull final float[] floats, final int size,
            @Nonnull final DataOutput out) throws IOException {
        for (int i = 0; i < size; i++) {
            int bits = Float.floatToIntBits(floats[i]);
            ZigZagLEB128Codec.writeSignedInt(bits, out);
        }
    }

    @Deprecated
    @Nonnull
    public static float[] readVFloats(@Nonnull final DataInput in, final int size)
            throws IOException {
        final float[] floats = new float[size];
        for (int i = 0; i < size; i++) {
            int bits = ZigZagLEB128Codec.readSignedInt(in);
            floats[i] = Float.intBitsToFloat(bits);
        }
        return floats;
    }

    public static void finishStream(@Nonnull final OutputStream out) throws IOException {
        if (out instanceof FinishableOutputStream) {
            ((FinishableOutputStream) out).finish();
        } else {
            out.flush();
        }
    }

    public static void readFully(final InputStream in, final byte[] b, int offset, int len)
            throws IOException {
        do {
            final int bytesRead = in.read(b, offset, len);
            if (bytesRead < 0) {
                throw new EOFException();
            }
            len -= bytesRead;
            offset += bytesRead;
        } while (len != 0);
    }

    public static void readFully(final InputStream in, final byte[] b) throws IOException {
        readFully(in, b, 0, b.length);
    }

}
