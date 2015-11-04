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
package hivemall.utils.io;

import java.io.InputStream;

/**
 * ByteArrayInputStream implementation that does not synchronize methods.
 */
public final class FastByteArrayInputStream extends InputStream {

    protected/* final */byte[] buf;
    protected/* final */int count;
    protected int pos = 0;

    public FastByteArrayInputStream() {}

    public FastByteArrayInputStream(byte[] buf) {
        this(buf, buf.length);
    }

    public FastByteArrayInputStream(byte[] buf, int count) {
        this.buf = buf;
        this.count = count;
    }

    public FastByteArrayInputStream(byte buf[], int offset, int length) {
        this.buf = buf;
        this.pos = offset;
        this.count = Math.min(offset + length, buf.length);
    }

    public void init(byte[] buf) {
        init(buf, buf.length);
    }

    public void init(byte[] buf, int count) {
        this.buf = buf;
        this.count = count;
        this.pos = 0;
    }

    @Override
    public final int available() {
        return count - pos;
    }

    public final int read() {
        return (pos < count) ? (buf[pos++] & 0xff) : -1;
    }

    @Override
    public final int read(byte[] b, int off, int len) {
        if(pos >= count) {
            return -1;
        }
        if((pos + len) > count) {
            len = (count - pos);
        }
        if(len <= 0) {
            return 0;
        }
        System.arraycopy(buf, pos, b, off, len);
        pos += len;
        return len;
    }

    @Override
    public final long skip(long n) {
        if((pos + n) > count) {
            n = count - pos;
        }
        if(n < 0) {
            return 0;
        }
        pos += n;
        return n;
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public void mark(int readlimit) {
        throw new UnsupportedOperationException();
    }

}
