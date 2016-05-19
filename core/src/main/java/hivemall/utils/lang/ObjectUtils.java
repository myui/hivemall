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

import hivemall.utils.io.FastByteArrayInputStream;
import hivemall.utils.io.FastByteArrayOutputStream;
import hivemall.utils.io.FastMultiByteArrayOutputStream;
import hivemall.utils.io.IOUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import javax.annotation.Nonnull;

public final class ObjectUtils {

    private ObjectUtils() {}

    public static byte[] toBytes(@Nonnull final Object obj) throws IOException {
        FastByteArrayOutputStream bos = new FastByteArrayOutputStream();
        toStream(obj, bos);
        return bos.toByteArray();
    }

    public static byte[] toBytes(@Nonnull final Externalizable obj) throws IOException {
        FastByteArrayOutputStream bos = new FastByteArrayOutputStream();
        toStream(obj, bos);
        return bos.toByteArray();
    }

    public static byte[] toCompressedBytes(@Nonnull final Object obj) throws IOException {
        FastMultiByteArrayOutputStream bos = new FastMultiByteArrayOutputStream();
        final DeflaterOutputStream dos = new DeflaterOutputStream(bos);
        try {
            toStream(obj, dos);
            return bos.toByteArray_clear();
        } finally {
            IOUtils.closeQuietly(dos);
        }
    }

    public static byte[] toCompressedBytes(@Nonnull final Externalizable obj) throws IOException {
        FastMultiByteArrayOutputStream bos = new FastMultiByteArrayOutputStream();
        final DeflaterOutputStream dos = new DeflaterOutputStream(bos);
        try {
            toStream(obj, dos);
            return bos.toByteArray_clear();
        } finally {
            IOUtils.closeQuietly(dos);
        }
    }

    public static void toStream(@Nonnull final Object obj, @Nonnull final OutputStream out)
            throws IOException {
        ObjectOutputStream oos = new ObjectOutputStream(out);
        oos.writeObject(obj);
        oos.flush();
    }

    public static void toStream(@Nonnull final Externalizable obj, @Nonnull final OutputStream out)
            throws IOException {
        ObjectOutputStream oos = new ObjectOutputStream(out);
        obj.writeExternal(oos);
        oos.flush();
    }

    public static <T> T readObject(@Nonnull final byte[] obj) throws IOException,
            ClassNotFoundException {
        return readObject(obj, obj.length);
    }

    public static <T> T readObject(@Nonnull final byte[] obj, final int length) throws IOException,
            ClassNotFoundException {
        return readObject(new FastByteArrayInputStream(obj, length));
    }

    public static void readObject(@Nonnull final byte[] src, @Nonnull final Externalizable dst)
            throws IOException, ClassNotFoundException {
        readObject(src, src.length, dst);
    }

    public static void readObject(@Nonnull final byte[] src, final int length,
            @Nonnull final Externalizable dst) throws IOException, ClassNotFoundException {
        readObject(new FastByteArrayInputStream(src, length), dst);
    }

    @SuppressWarnings("unchecked")
    public static <T> T readObject(@Nonnull final InputStream is) throws IOException,
            ClassNotFoundException {
        ObjectInputStream ois = new ObjectInputStream(is);
        return (T) ois.readObject();
    }

    public static void readObject(@Nonnull final InputStream is, @Nonnull final Externalizable dst)
            throws IOException, ClassNotFoundException {
        ObjectInputStream ois = new ObjectInputStream(is);
        dst.readExternal(ois);
    }

    public static <T> T readCompressedObject(@Nonnull final byte[] obj) throws IOException,
            ClassNotFoundException {
        FastByteArrayInputStream bis = new FastByteArrayInputStream(obj);
        final InflaterInputStream iis = new InflaterInputStream(bis);
        try {
            return readObject(iis);
        } finally {
            IOUtils.closeQuietly(iis);
        }
    }

    public static void readCompressedObject(@Nonnull final byte[] src,
            @Nonnull final Externalizable dst) throws IOException, ClassNotFoundException {
        FastByteArrayInputStream bis = new FastByteArrayInputStream(src);
        final InflaterInputStream iis = new InflaterInputStream(bis);
        try {
            readObject(iis, dst);
        } finally {
            IOUtils.closeQuietly(iis);
        }
    }

    public static void readCompressedObject(@Nonnull final byte[] src, final int offset,
            final int length, @Nonnull final Externalizable dst) throws IOException,
            ClassNotFoundException {
        FastByteArrayInputStream bis = new FastByteArrayInputStream(src, offset, length);
        final InflaterInputStream iis = new InflaterInputStream(bis);
        try {
            readObject(iis, dst);
        } finally {
            IOUtils.closeQuietly(iis);
        }
    }

    public static <E extends Enum<E>> E fromOrdinal(Class<E> enumClass, int ordinal) {
        E[] enumArray = enumClass.getEnumConstants();
        return enumArray[ordinal];
    }

}
