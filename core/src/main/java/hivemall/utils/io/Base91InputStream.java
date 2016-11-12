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

import hivemall.utils.codec.Base91;
import hivemall.utils.codec.Base91.Base91Buf;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class Base91InputStream extends FilterInputStream {
    private static final int INPUT_BUFFER_SIZE = 2048;

    private/* final */byte[] inputBuffer;
    private/* final */FastByteArrayOutputStream outputBuffer;
    @Nonnull
    private final Base91Buf decodingBuf;

    @Nullable
    private byte[] output;
    private int outputPos;
    private int outputLen;
    private boolean eof;

    public Base91InputStream(@Nonnull InputStream in) {
        super(in);
        this.inputBuffer = new byte[INPUT_BUFFER_SIZE];
        int outputBufferSize = Math.round(INPUT_BUFFER_SIZE / Base91.BEST_ENCODING_RATIO);
        this.outputBuffer = new FastByteArrayOutputStream(outputBufferSize);
        this.decodingBuf = new Base91Buf();
        this.output = null;
        this.outputPos = 0;
        this.outputLen = 0;
        this.eof = false;
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public void mark(int readlimit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int available() {
        return eof ? 0 : 1;
    }

    @Override
    public long skip(long n) throws IOException {
        throw new UnsupportedOperationException("Skip is not supported");
    }

    @Override
    public int read() throws IOException {
        if (outputPos >= outputLen) {
            if (refill() == false) {// reached EOF
                if (!decodingBuf.isEmpty()) {
                    return Base91.decodeEnd(decodingBuf);
                }
            }
        }
        if (outputPos >= outputLen) {
            return -1;
        }
        return output[outputPos++] & 0xff;
    }

    @Override
    public int read(@Nonnull final byte[] b, final int off, final int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || off + len > b.length) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        if (outputPos >= outputLen) {
            if (refill() == false) {// reached EOF
                if (!decodingBuf.isEmpty()) {
                    byte last = Base91.decodeEnd(decodingBuf);
                    b[off] = last;
                    return 1;
                }
            }
        }
        if (outputPos >= outputLen) {
            return -1;
        }
        int bytes = Math.min(len, outputLen - outputPos);
        System.arraycopy(output, outputPos, b, off, bytes);
        this.outputPos += bytes;
        return bytes;
    }

    @Override
    public void close() throws IOException {
        in.close();
        this.inputBuffer = null;
        this.outputBuffer = null;
        this.output = null;
    }

    private boolean refill() throws IOException {
        if (eof) {
            return false;
        }

        int bytesRead = in.read(inputBuffer);
        if (bytesRead == -1) {
            this.eof = true;
            this.output = null;
            return false;
        }

        outputBuffer.reset();
        Base91.decode(inputBuffer, 0, bytesRead, outputBuffer, decodingBuf);

        this.output = outputBuffer.getInternalArray();
        this.outputPos = 0;
        this.outputLen = outputBuffer.size();
        return true;
    }

}
