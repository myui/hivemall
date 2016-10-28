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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.LinkedList;

/**
 * ByteArrayOutputStream implementation that doesn't synchronize methods.
 */
public final class FastMultiByteArrayOutputStream extends OutputStream {
    private static final int DEFAULT_BLOCK_SIZE = 8192;

    private final int blockSize;

    private byte[] buffer;
    private int size;

    private LinkedList<byte[]> buffers;
    private int index;

    public FastMultiByteArrayOutputStream() {
        this(DEFAULT_BLOCK_SIZE);
    }

    public FastMultiByteArrayOutputStream(int aSize) {
        this.blockSize = aSize;
        this.buffer = new byte[aSize];
    }

    public int size() {
        return size + index;
    }

    public byte[][] toMultiByteArray() {
        if (buffers == null) {
            final byte[][] mb = new byte[1][];
            mb[0] = buffer;
            return mb;
        }
        final int listsize = buffers.size();
        final int size = listsize + 1;
        final byte[][] mb = new byte[size][];
        buffers.toArray(mb);
        mb[listsize] = buffer;
        return mb;
    }

    public byte[] toByteArray() {
        final byte[] data = new byte[size()];
        // check if we have a list of buffers
        int pos = 0;
        if (buffers != null) {
            for (byte[] bytes : buffers) {
                System.arraycopy(bytes, 0, data, pos, blockSize);
                pos += blockSize;
            }
        }
        // write the internal buffer directly
        System.arraycopy(buffer, 0, data, pos, index);
        return data;
    }

    public byte[] toByteArray_clear() {
        final int size = size();
        final byte[] data = new byte[size];
        // check if we have a list of buffers
        int pos = 0;
        if (buffers != null) {
            while (!buffers.isEmpty()) {
                byte[] bytes = buffers.removeFirst();
                System.arraycopy(bytes, 0, data, pos, blockSize);
                pos += blockSize;
            }
        }
        // write the internal buffer directly
        System.arraycopy(buffer, 0, data, pos, index);
        return data;
    }

    @Override
    public String toString() {
        return new String(toByteArray());
    }

    public String toString(String enc) throws UnsupportedEncodingException {
        return new String(toByteArray(), enc);
    }

    @Override
    public void write(int datum) {
        if (index >= blockSize) {
            // Create new buffer and store current in linked list
            addBuffer();
        }
        // store the byte
        buffer[index++] = (byte) datum;
    }

    @Override
    public void write(byte[] data, int offset, int length) {
        if (data == null) {
            throw new NullPointerException();
        } else if ((offset < 0) || (offset + length > data.length) || (length < 0)) {
            throw new IndexOutOfBoundsException();
        } else {
            if (index + length >= blockSize) {// Write byte by byte
                int copyLength;
                do {
                    if (index == blockSize) {
                        addBuffer();
                    }
                    copyLength = blockSize - index;
                    if (length < copyLength) {
                        copyLength = length;
                    }
                    System.arraycopy(data, offset, buffer, index, copyLength);
                    offset += copyLength;
                    index += copyLength;
                    length -= copyLength;
                } while (length > 0);
            } else {// copy in the subarray
                System.arraycopy(data, offset, buffer, index, length);
                index += length;
            }
        }
    }

    public void writeInt(int v) throws IOException {
        if ((index + 3) >= blockSize) {
            // Create new buffer and store current in linked list
            addBuffer();
        }
        buffer[index++] = (byte) (v >>> 24);
        buffer[index++] = (byte) (v >>> 16);
        buffer[index++] = (byte) (v >>> 8);
        buffer[index++] = (byte) (v >>> 0);
    }

    public void writeLong(long v) throws IOException {
        if ((index + 7) >= blockSize) {
            // Create new buffer and store current in linked list
            addBuffer();
        }
        buffer[index++] = (byte) (v >>> 56);
        buffer[index++] = (byte) (v >>> 48);
        buffer[index++] = (byte) (v >>> 40);
        buffer[index++] = (byte) (v >>> 32);
        buffer[index++] = (byte) (v >>> 24);
        buffer[index++] = (byte) (v >>> 16);
        buffer[index++] = (byte) (v >>> 8);
        buffer[index++] = (byte) (v >>> 0);
    }

    public void writeInts(int[] v, int off, int len) throws IOException {
        int endoff = off + len;
        for (int i = off; i < endoff; i++) {
            writeInt(v[i]);
        }
    }

    public void writeLongs(long[] v, int off, int len) throws IOException {
        int endoff = off + len;
        for (int i = off; i < endoff; i++) {
            writeLong(v[i]);
        }
    }

    public void reset() {
        buffer = new byte[blockSize];
        buffers = null;
        size = 0;
        index = 0;
    }

    public void writeTo(OutputStream out) throws IOException {
        // check if we have a list of buffers
        if (buffers != null) {
            for (byte[] bytes : buffers) {
                out.write(bytes, 0, blockSize);
            }
        }
        // write the internal buffer directly
        out.write(buffer, 0, index);
    }

    public InputStream getInputStream() {
        return new FastByteArrayInputStream(toByteArray());
    }

    /**
     * Create a new buffer and store the current one in linked list.
     */
    private void addBuffer() {
        if (buffers == null) {
            buffers = new LinkedList<byte[]>();
        }
        buffers.addLast(buffer);
        buffer = new byte[blockSize];
        size += index;
        index = 0;
    }

    @Override
    public void close() throws IOException {
        clear();
    }

    public void clear() {
        this.buffer = null;
        this.buffers = null;
    }

}
