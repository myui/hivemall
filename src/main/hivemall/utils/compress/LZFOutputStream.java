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
import java.io.OutputStream;

public final class LZFOutputStream extends OutputStream {

    public static final int MAGIC = ('L' << 24) | ('Z' << 16) | ('F' << 8) | '0';
    public static final int DEFAULT_BUF_SIZE = 4096;

    private final LZFCodec _codec = new LZFCodec();

    private final OutputStream _out;
    private final int _buflen;
    private final byte[] _buffer;
    private int _pos = 0;

    public LZFOutputStream(OutputStream out) {
        this(out, DEFAULT_BUF_SIZE);
    }

    public LZFOutputStream(OutputStream out, int buflen) {
        this(out, buflen, false);
    }

    public LZFOutputStream(OutputStream out, int buflen, boolean noMagic) {
        this._out = out;
        this._buflen = buflen;
        this._buffer = new byte[buflen];
        if(!noMagic) {
            try {
                writeInt(MAGIC, out);
            } catch (IOException e) {
                throw new IllegalStateException("failed to write a magic field", e);
            }
        }
    }

    @Override
    public void write(int b) throws IOException {
        if(_pos >= _buflen) {
            compressAndWrite(_buffer, _pos);
            _pos = 0;
        }
        _buffer[_pos++] = (byte) b;
    }

    @Override
    public void flush() throws IOException {
        compressAndWrite(_buffer, _pos);
        _pos = 0;
        _out.flush();
    }

    @Override
    public void close() throws IOException {
        flush();
        _out.close();
    }

    private void compressAndWrite(final byte[] buff, final int len) throws IOException {
        assert (len <= DEFAULT_BUF_SIZE) : buff;
        if(len > 0) {
            final byte[] compressed = _codec.compress(buff, len);
            final int clen = compressed.length;
            if(clen >= len) { // no compress
                writeInt(-len, _out);
                _out.write(buff, 0, len);
            } else {// compress
                writeInt(clen, _out);
                _out.write(compressed, 0, clen);
            }
        }
    }

    private void writeInt(final int x, final OutputStream out) throws IOException {
        out.write((byte) (x >>> 24));
        out.write((byte) (x >>> 16));
        out.write((byte) (x >>> 8));
        out.write((byte) (x >>> 0));
    }

}
