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

import hivemall.utils.lang.ArrayUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import javax.annotation.CheckForNull;

/**
 * Unsynchronized version of {@link ByteArrayOutputStream}.
 */
public final class FastByteArrayOutputStream extends OutputStream {
    private static final int DEFAULT_BLOCK_SIZE = 8192;

    private byte[] buf;
    private int count;

    public FastByteArrayOutputStream() {
        this(DEFAULT_BLOCK_SIZE);
    }

    public FastByteArrayOutputStream(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("Negative initial size: " + size);
        }
        this.buf = new byte[size];
        this.count = 0;
    }

    public FastByteArrayOutputStream(@CheckForNull byte[] buf) {
        if (buf == null) {
            throw new IllegalArgumentException("buf should not be NULL");
        }
        this.buf = buf;
        this.count = 0;
    }

    public byte[] getInternalArray() {
        return buf;
    }

    public byte[] toByteArray() {
        return ArrayUtils.copyOf(buf, count);
    }

    public void write(int b) {
        int newcount = count + 1;
        if (newcount > buf.length) {
            buf = ArrayUtils.copyOf(buf, Math.max(buf.length << 1, newcount));
        }
        buf[count] = (byte) b;
        this.count = newcount;
    }

    @Override
    public void write(byte b[], int off, int len) {
        if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length)
                || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }
        int newcount = count + len;
        if (newcount > buf.length) {
            buf = ArrayUtils.copyOf(buf, Math.max(buf.length << 1, newcount));
        }
        System.arraycopy(b, off, buf, count, len);
        this.count = newcount;
    }

    public void write(ByteBuffer b) {
        write(b, b.remaining());
    }

    public void write(ByteBuffer b, int len) {
        final int newcount = count + len;
        if (newcount > buf.length) {
            buf = ArrayUtils.copyOf(buf, Math.max(buf.length << 1, newcount));
        }
        b.get(buf, count, len);
        this.count = newcount;
    }

    public void writeTo(OutputStream out) throws IOException {
        out.write(buf, 0, count);
    }

    public void reset() {
        this.count = 0;
    }

    public void reset(int size) {
        this.buf = new byte[size];
        this.count = 0;
    }

    public int size() {
        return count;
    }

    @Override
    public void close() throws IOException {
        clear();
    }

    public void clear() {
        this.buf = null;
    }

    @Override
    public String toString() {
        return new String(toByteArray());
    }

}
