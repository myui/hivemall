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

import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.Nonnull;

public final class Base91OutputStream extends FinishableOutputStreamAdapter {
    private static final int INPUT_BUFFER_SIZE = 2048;

    private/* final */byte[] inputBuffer;
    private int inputPos;
    @Nonnull
    private final Base91Buf encodingBuf;

    public Base91OutputStream(@Nonnull OutputStream out) {
        super(out);
        this.inputBuffer = new byte[INPUT_BUFFER_SIZE];
        this.inputPos = 0;
        this.encodingBuf = new Base91Buf();
    }

    @Override
    public void write(final int b) throws IOException {
        if (inputPos >= inputBuffer.length) {
            flushBuffer();
        }
        inputBuffer[inputPos++] = (byte) b;
    }

    @Override
    public void write(@Nonnull final byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(@Nonnull final byte[] b, final int off, final int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || off + len > b.length) {
            throw new IndexOutOfBoundsException();
        }

        for (int i = 0; i < len; i++) {
            write(b[off + i]);
        }
    }

    @Override
    public void close() throws IOException {
        IOException thrown = null;
        try {
            flushBuffer();
        } catch (IOException e) {
            thrown = e;
        }
        try {
            _out.flush();
            _out.close();
        } catch (IOException e) {
            if (thrown != null) {
                thrown = e;
            }
        }
        this.inputBuffer = null;
        if (thrown != null) {
            throw thrown;
        }
    }

    private void flushBuffer() throws IOException {
        if (inputPos > 0) {
            internalWrite(inputBuffer, 0, inputPos);
            this.inputPos = 0;
        }
    }

    private void internalWrite(@Nonnull final byte[] input, final int offset, final int length)
            throws IOException {
        Base91.encode(input, offset, length, _out, encodingBuf);
    }

    @Override
    public void finish() throws IOException {
        flushBuffer();
        Base91.encodeEnd(_out, encodingBuf);
        _out.flush();
    }

}
