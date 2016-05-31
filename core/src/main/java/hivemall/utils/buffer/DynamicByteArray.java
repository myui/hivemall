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
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hivemall.utils.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A class that is a growable array of bytes. Growth is managed in terms of chunks that are
 * allocated when needed.
 * 
 * This class is based on <code>org.apache.orc.impl.DynamicByteArray</code>.
 */
public final class DynamicByteArray {
    static final int DEFAULT_CHUNKSIZE = 32 * 1024;
    static final int DEFAULT_NUM_CHUNKS = 128;

    private final int chunkSize; // our allocation sizes
    private byte[][] data; // the real data
    private int length; // max set element index +1
    private int initializedChunks = 0; // the number of chunks created

    public DynamicByteArray() {
        this(DEFAULT_NUM_CHUNKS, DEFAULT_CHUNKSIZE);
    }

    public DynamicByteArray(int numChunks, int chunkSize) {
        if (chunkSize == 0) {
            throw new IllegalArgumentException("bad chunksize");
        }
        this.chunkSize = chunkSize;
        this.data = new byte[numChunks][];
    }

    /**
     * Ensure that the given index is valid.
     */
    private void grow(final int chunkIndex) {
        if (chunkIndex >= initializedChunks) {
            if (chunkIndex >= data.length) {
                int newSize = Math.max(chunkIndex + 1, 2 * data.length);
                byte[][] newChunk = new byte[newSize][];
                System.arraycopy(data, 0, newChunk, 0, data.length);
                this.data = newChunk;
            }
            for (int i = initializedChunks; i <= chunkIndex; ++i) {
                data[i] = new byte[chunkSize];
            }
            this.initializedChunks = chunkIndex + 1;
        }
    }

    public byte get(final int index) {
        if (index >= length) {
            throw new IndexOutOfBoundsException("Index " + index + " is outside of 0.."
                    + (length - 1));
        }
        int i = index / chunkSize;
        int j = index % chunkSize;
        return data[i][j];
    }

    public void get(@Nonnull final ByteBuffer result, final int offset, int length) {
        result.clear();
        int currentChunk = offset / chunkSize;
        int currentOffset = offset % chunkSize;
        int currentLength = Math.min(length, chunkSize - currentOffset);
        while (length > 0) {
            result.put(data[currentChunk], currentOffset, currentLength);
            length -= currentLength;
            currentChunk += 1;
            currentOffset = 0;
            currentLength = Math.min(length, chunkSize - currentOffset);
        }
    }

    public int getInt(final int index) {
        return toInt(get(index), get(index + 1), get(index + 2), get(index + 3));
    }

    public long getLong(final int index) {
        return toLong(get(index), get(index + 1), get(index + 2), get(index + 3), get(index + 4),
            get(index + 5), get(index + 6), get(index + 7));
    }

    public float getFloat(final int index) {
        return Float.intBitsToFloat(getInt(index));
    }

    public double getDouble(final int index) {
        return Double.longBitsToDouble(getLong(index));
    }

    private static int toInt(final byte b3, final byte b2, final byte b1, final byte b0) {
        return (((b3) << 24) | ((b2 & 0xff) << 16) | ((b1 & 0xff) << 8) | ((b0 & 0xff)));
    }

    private static long toLong(final byte b7, final byte b6, final byte b5, final byte b4,
            final byte b3, final byte b2, final byte b1, final byte b0) {
        return ((((long) b7) << 56) | (((long) b6 & 0xff) << 48) | (((long) b5 & 0xff) << 40)
                | (((long) b4 & 0xff) << 32) | (((long) b3 & 0xff) << 24)
                | (((long) b2 & 0xff) << 16) | (((long) b1 & 0xff) << 8) | (((long) b0 & 0xff)));
    }

    public void setInt(final int index, final int value) {
        set(index, int3(value));
        set(index + 1, int2(value));
        set(index + 2, int1(value));
        set(index + 3, int0(value));
    }

    public void setLong(final int index, final long value) {
        set(index, long7(value));
        set(index + 1, long6(value));
        set(index + 2, long5(value));
        set(index + 3, long4(value));
        set(index + 4, long3(value));
        set(index + 5, long2(value));
        set(index + 6, long1(value));
        set(index + 7, long0(value));
    }

    public void setFloat(final int index, final float value) {
        setInt(index, Float.floatToIntBits(value));
    }

    public void setDouble(final int index, final double value) {
        setLong(index, Double.doubleToLongBits(value));
    }

    private static byte int3(final int x) {
        return (byte) (x >> 24);
    }

    private static byte int2(final int x) {
        return (byte) (x >> 16);
    }

    private static byte int1(final int x) {
        return (byte) (x >> 8);
    }

    private static byte int0(final int x) {
        return (byte) (x);
    }

    private static byte long7(final long x) {
        return (byte) (x >> 56);
    }

    private static byte long6(final long x) {
        return (byte) (x >> 48);
    }

    private static byte long5(final long x) {
        return (byte) (x >> 40);
    }

    private static byte long4(final long x) {
        return (byte) (x >> 32);
    }

    private static byte long3(final long x) {
        return (byte) (x >> 24);
    }

    private static byte long2(final long x) {
        return (byte) (x >> 16);
    }

    private static byte long1(final long x) {
        return (byte) (x >> 8);
    }

    private static byte long0(final long x) {
        return (byte) (x);
    }

    public void set(final int index, final byte value) {
        int i = index / chunkSize;
        int j = index % chunkSize;
        grow(i);
        if (index >= length) {
            length = index + 1;
        }
        data[i][j] = value;
    }

    public int add(final byte value) {
        int i = length / chunkSize;
        int j = length % chunkSize;
        grow(i);
        data[i][j] = value;
        int result = length;
        length += 1;
        return result;
    }

    /**
     * Copy a slice of a byte array into our buffer.
     * 
     * @param value the array to copy from
     * @param valueOffset the first location to copy from value
     * @param valueLength the number of bytes to copy from value
     * @return the offset of the start of the value
     */
    public int add(@Nonnull final byte[] value, int valueOffset, final int valueLength) {
        int i = length / chunkSize;
        int j = length % chunkSize;
        grow((length + valueLength) / chunkSize);
        int remaining = valueLength;
        while (remaining > 0) {
            int size = Math.min(remaining, chunkSize - j);
            System.arraycopy(value, valueOffset, data[i], j, size);
            remaining -= size;
            valueOffset += size;
            i += 1;
            j = 0;
        }
        int result = length;
        length += valueLength;
        return result;
    }

    /**
     * Copy a slice of a byte array into our buffer.
     * 
     * @param src the buffer to copy from
     * @param valueOffset the first location to copy from value
     * @param valueLength the number of bytes to copy from value
     * @return the offset of the start of the value
     */
    public int add(@Nonnull final ByteBuffer src, int valueOffset, final int valueLength) {
        int i = length / chunkSize;
        int j = length % chunkSize;
        grow((length + valueLength) / chunkSize);
        int remaining = valueLength;
        while (remaining > 0) {
            int size = Math.min(remaining, chunkSize - j);
            src.get(data[i], j, size);
            remaining -= size;
            valueOffset += size;
            i += 1;
            j = 0;
        }
        int result = length;
        length += valueLength;
        return result;
    }

    /**
     * Byte compare a set of bytes against the bytes in this dynamic array.
     * 
     * @param other source of the other bytes
     * @param otherOffset start offset in the other array
     * @param otherLength number of bytes in the other array
     * @param ourOffset the offset in our array
     * @param ourLength the number of bytes in our array
     * @return negative for less, 0 for equal, positive for greater
     */
    public int compare(final byte[] other, int otherOffset, final int otherLength,
            final int ourOffset, final int ourLength) {
        int currentChunk = ourOffset / chunkSize;
        int currentOffset = ourOffset % chunkSize;
        int maxLength = Math.min(otherLength, ourLength);
        while (maxLength > 0 && other[otherOffset] == data[currentChunk][currentOffset]) {
            otherOffset += 1;
            currentOffset += 1;
            if (currentOffset == chunkSize) {
                currentChunk += 1;
                currentOffset = 0;
            }
            maxLength -= 1;
        }
        if (maxLength == 0) {
            return otherLength - ourLength;
        }
        int otherByte = 0xff & other[otherOffset];
        int ourByte = 0xff & data[currentChunk][currentOffset];
        return otherByte > ourByte ? 1 : -1;
    }

    /**
     * Get the size of the array.
     * 
     * @return the number of bytes in the array
     */
    public int size() {
        return length;
    }

    /**
     * Clear the array to its original pristine state.
     */
    public void clear() {
        this.length = 0;
        for (int i = 0; i < data.length; ++i) {
            data[i] = null;
        }
        this.initializedChunks = 0;
    }

    /**
     * Read the entire stream into this array.
     * 
     * @param in the stream to read from
     * @throws IOException
     */
    public void read(@Nonnull final InputStream in) throws IOException {
        int currentChunk = length / chunkSize;
        int currentOffset = length % chunkSize;
        grow(currentChunk);
        int currentLength = in.read(data[currentChunk], currentOffset, chunkSize - currentOffset);
        while (currentLength > 0) {
            length += currentLength;
            currentOffset = length % chunkSize;
            if (currentOffset == 0) {
                currentChunk = length / chunkSize;
                grow(currentChunk);
            }
            currentLength = in.read(data[currentChunk], currentOffset, chunkSize - currentOffset);
        }
    }

    /**
     * Write out a range of this dynamic array to an output stream.
     * 
     * @param out the stream to write to
     * @param offset the first offset to write
     * @param length the number of bytes to write
     * @throws IOException
     */
    public void write(@Nonnull final OutputStream out, final int offset, int length)
            throws IOException {
        int currentChunk = offset / chunkSize;
        int currentOffset = offset % chunkSize;
        while (length > 0) {
            int currentLength = Math.min(length, chunkSize - currentOffset);
            out.write(data[currentChunk], currentOffset, currentLength);
            length -= currentLength;
            currentChunk += 1;
            currentOffset = 0;
        }
    }

    @Override
    public String toString() {
        int i;
        StringBuilder sb = new StringBuilder(length * 3);

        sb.append('{');
        int l = length - 1;
        for (i = 0; i < l; i++) {
            sb.append(Integer.toHexString(get(i)));
            sb.append(',');
        }
        sb.append(Integer.toHexString(get(i)));
        sb.append('}');

        return sb.toString();
    }

    /**
     * Gets all the bytes of the array.
     *
     * @return Bytes of the array
     */
    @Nullable
    public byte[] get() {
        byte[] result = null;
        if (length > 0) {
            int currentChunk = 0;
            int currentOffset = 0;
            int currentLength = Math.min(length, chunkSize);
            int destOffset = 0;
            result = new byte[length];
            int totalLength = length;
            while (totalLength > 0) {
                System.arraycopy(data[currentChunk], currentOffset, result, destOffset,
                    currentLength);
                destOffset += currentLength;
                totalLength -= currentLength;
                currentChunk += 1;
                currentOffset = 0;
                currentLength = Math.min(totalLength, chunkSize - currentOffset);
            }
        }
        return result;
    }

    /**
     * Get the size of the buffers.
     */
    public long getSizeInBytes() {
        return initializedChunks * chunkSize;
    }
}
