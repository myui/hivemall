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

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * This class represents an ASCII85 output stream. This class is based on the implementation in
 * Apache PDFBox.
 */
public final class ASCII85OutputStream extends FilterOutputStream {

    private static final long a85p2 = 85L * 85L;
    private static final long a85p3 = 85L * 85L * 85L;
    private static final long a85p4 = 85L * 85L * 85L * 85L;
    private static final byte TERMINATOR = '~';
    private static final byte OFFSET = '!';
    private static final byte Z = 'z';

    private final byte[] indata;
    private final byte[] encoded;

    private int count;
    private boolean flushed;

    /**
     * Constructor.
     *
     * @param out The output stream to write to.
     */
    public ASCII85OutputStream(OutputStream out) {
        super(out);
        indata = new byte[4];
        encoded = new byte[5];
        count = 0;
        flushed = true;
    }

    /**
     * This will transform the next four ascii bytes.
     */
    private void transformASCII85() {
        long word = ((((indata[0] << 8) | (indata[1] & 0xFF)) << 16) | ((indata[2] & 0xFF) << 8) | (indata[3] & 0xFF)) & 0xFFFFFFFFL;

        if (word == 0) {
            encoded[0] = Z;
            encoded[1] = 0;
            return;
        }

        long x = word / a85p4;
        encoded[0] = (byte) (x + OFFSET);
        word -= x * a85p4;

        x = word / a85p3;
        encoded[1] = (byte) (x + OFFSET);
        word -= x * a85p3;

        x = word / a85p2;
        encoded[2] = (byte) (x + OFFSET);
        word -= x * a85p2;

        x = word / 85L;
        encoded[3] = (byte) (x + OFFSET);

        encoded[4] = (byte) ((word % 85L) + OFFSET);
    }

    /**
     * This will write a single byte.
     *
     * @param b The byte to write.
     *
     * @throws IOException If there is an error writing to the stream.
     */
    @Override
    public void write(int b) throws IOException {
        flushed = false;
        indata[count++] = (byte) b;
        if (count < 4) {
            return;
        }
        transformASCII85();
        for (int i = 0; i < 5; i++) {
            if (encoded[i] == 0) {
                break;
            }
            out.write(encoded[i]);
        }
        count = 0;
    }

    /**
     * This will flush the data to the stream.
     *
     * @throws IOException If there is an error writing the data to the stream.
     */
    @Override
    public void flush() throws IOException {
        if (flushed) {
            return;
        }
        if (count > 0) {
            for (int i = count; i < 4; i++) {
                indata[i] = 0;
            }
            transformASCII85();
            if (encoded[0] == Z) {
                for (int i = 0; i < 5; i++) {
                    encoded[i] = OFFSET;// expand 'z',
                }
            }
            for (int i = 0; i < count + 1; i++) {
                out.write(encoded[i]);
            }
        }
        out.write(TERMINATOR);
        count = 0;
        flushed = true;
        super.flush();
    }

    /**
     * This will close the stream.
     *
     * @throws IOException If there is an error closing the wrapped stream.
     */
    @Override
    public void close() throws IOException {
        flush();
        super.close();
    }
}
