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

import java.io.IOException;
import java.io.InputStream;

public final class LZFInputStream extends InputStream {

    private final LZFCodec _codec = new LZFCodec();

    private final InputStream _in;
    private byte[] _buffer;
    private int _pos = 0;

    public LZFInputStream(InputStream in) throws IOException {
        this(in, LZFOutputStream.DEFAULT_BUF_SIZE);
    }

    public LZFInputStream(InputStream in, int buflen) throws IOException {
        this(in, buflen, false);
    }

    public LZFInputStream(InputStream in, int buflen, boolean noMagic) throws IOException {
        if(!noMagic && (readInt(in) != LZFOutputStream.MAGIC)) {
            throw new IllegalStateException("Not a LZF Stream");
        }
        this._in = in;
    }

    @Override
    public int read() throws IOException {
        fillBuffer();
        final byte[] buf = _buffer;
        if(_pos >= buf.length) {
            return -1;
        }
        return buf[_pos++] & 255;
    }

    private void fillBuffer() throws IOException {
        final byte[] buffer = _buffer;
        if(buffer != null && _pos < buffer.length) {
            return;
        }
        final InputStream in = _in;
        int len = readInt(in);
        assert (len <= LZFOutputStream.DEFAULT_BUF_SIZE) : len;
        if(len < 0) { // not compressed
            len = -len;
            byte[] dest = ensureBuffer(buffer, len);
            readFully(in, len, dest);
            this._buffer = dest;
        } else {
            byte[] dest = ensureBuffer(buffer, len);
            readFully(in, len, dest);
            byte[] uncompressed = _codec.decompress(dest);
            this._buffer = uncompressed;
        }
        this._pos = 0;
    }

    private static int readInt(final InputStream in) throws IOException {
        int x = ((in.read() & 0xFF) << 24);
        x += ((in.read() & 0xFF) << 16);
        x += ((in.read() & 0xFF) << 8);
        x += (in.read() & 0xFF);
        return x;
    }

    private static byte[] ensureBuffer(final byte[] buf, final int required) {
        return (buf == null || buf.length < required) ? new byte[required] : buf;
    }

    private static void readFully(final InputStream in, final int len, final byte[] dest)
            throws IOException {
        in.read(dest, 0, len);
    }

    @Override
    public void close() throws IOException {
        _in.close();
    }

    @Override
    public boolean markSupported() {
        return false;
    }

}
