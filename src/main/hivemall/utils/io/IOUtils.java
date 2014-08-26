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
package hivemall.utils.io;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

public final class IOUtils {

    private IOUtils() {}

    public static byte[] readBytes(final DataInput in) throws IOException {
        final int len = in.readInt();
        if(len == -1) {
            return null;
        }
        final byte[] b = new byte[len];
        in.readFully(b, 0, len);
        return b;
    }

    public static void writeBytes(final byte[] b, final DataOutput out) throws IOException {
        if(b == null) {
            out.writeInt(-1);
            return;
        }
        final int len = b.length;
        out.writeInt(len);
        out.write(b, 0, len);
    }

    public static String readString(final DataInput in) throws IOException {
        final int len = in.readInt();
        if(len == -1) {
            return null;
        }
        final char[] ch = new char[len];
        for(int i = 0; i < len; i++) {
            ch[i] = in.readChar();
        }
        return new String(ch);
    }

    public static void writeString(final String s, final DataOutput out) throws IOException {
        if(s == null) {
            out.writeInt(-1);
            return;
        }
        final int len = s.length();
        out.writeInt(len);
        for(int i = 0; i < len; i++) {
            int v = s.charAt(i);
            out.writeChar(v);
        }
    }

    public static <T extends Enum<T>> T readEnum(DataInput in, Class<T> enumType)
            throws IOException {
        String name = readString(in);
        return T.valueOf(enumType, name);
    }

    public static void writeEnum(Enum<?> enumVal, DataOutput out) throws IOException {
        String name = enumVal.name();
        writeString(name, out);
    }

    public static ObjectInputStream asObjectInputStream(DataInput in) throws IOException {
        if(in instanceof ObjectInputStream) {
            return (ObjectInputStream) in;
        }
        if(in instanceof InputStream) {
            InputStream is = (InputStream) in;
            return new ObjectInputStream(is);
        }
        throw new IllegalStateException();
    }

    public static ObjectOutputStream asObjectOutputStream(DataOutput out) throws IOException {
        if(out instanceof ObjectOutputStream) {
            return (ObjectOutputStream) out;
        }
        if(out instanceof OutputStream) {
            OutputStream os = (OutputStream) out;
            return new ObjectOutputStream(os);
        }
        throw new IllegalStateException();
    }

    public static void closeQuietly(final Closeable channel) {
        if(channel != null) {
            try {
                channel.close();
            } catch (IOException e) {
                ;
            }
        }
    }

}
