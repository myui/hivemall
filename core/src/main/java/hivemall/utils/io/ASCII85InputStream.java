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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * This class represents an ASCII85 stream. This class is based on the implementation in Apache
 * PDFBox.
 */
public final class ASCII85InputStream extends FilterInputStream {

    private static final byte TERMINATOR = '~';
    private static final byte OFFSET = '!';
    private static final byte NEWLINE = '\n';
    private static final byte RETURN = '\r';
    private static final byte SPACE = ' ';
    private static final byte PADDING_U = 'u';
    private static final byte Z = 'z';

    private int index;
    private int n;
    private boolean eof;

    private final byte[] ascii;
    private final byte[] decoded;

    /**
     * Constructor.
     *
     * @param is The input stream to actually read from.
     */
    public ASCII85InputStream(InputStream is) {
        super(is);
        index = 0;
        n = 0;
        eof = false;
        ascii = new byte[5];
        decoded = new byte[4];
    }

    /**
     * This will read the next byte from the stream.
     *
     * @return The next byte read from the stream.
     * @throws IOException If there is an error reading from the wrapped stream.
     */
    @Override
    public int read() throws IOException {
        if (index < n) {
            return decoded[index++] & 0xFF;
        }
        if (eof) {
            return -1;
        }

        index = 0;
        int k;
        byte z;
        do {
            int zz = (byte) in.read();
            if (zz == -1) {
                eof = true;
                return -1;
            }
            z = (byte) zz;
        } while (z == NEWLINE || z == RETURN || z == SPACE);

        if (z == TERMINATOR) {
            eof = true;
            n = 0;
            return -1;
        } else if (z == Z) {
            decoded[0] = decoded[1] = decoded[2] = decoded[3] = 0;
            n = 4;
        } else {
            ascii[0] = z; // may be EOF here....
            for (k = 1; k < 5; ++k) {
                do {
                    int zz = (byte) in.read();
                    if (zz == -1) {
                        eof = true;
                        return -1;
                    }
                    z = (byte) zz;
                } while (z == NEWLINE || z == RETURN || z == SPACE);
                ascii[k] = z;
                if (z == TERMINATOR) {
                    // don't include ~ as padding byte
                    ascii[k] = PADDING_U;
                    break;
                }
            }
            n = k - 1;
            if (n == 0) {
                eof = true;
                return -1;
            }
            if (k < 5) {
                for (++k; k < 5; ++k) {
                    ascii[k] = PADDING_U;
                }
                eof = true;
            }
            // decode stream
            long t = 0;
            for (k = 0; k < 5; ++k) {
                z = (byte) (ascii[k] - OFFSET);
                if (z < 0 || z > 93) {
                    throw new IOException("Invalid data in Ascii85 stream");
                }
                t = (t * 85L) + z;
            }
            for (k = 3; k >= 0; --k) {
                decoded[k] = (byte) (t & 0xFFL);
                t >>>= 8;
            }
        }
        return decoded[index++] & 0xFF;
    }

    /**
     * This will read a chunk of data.
     *
     * @param data The buffer to write data to.
     * @param offset The offset into the data stream.
     * @param len The number of byte to attempt to read.
     *
     * @return The number of bytes actually read.
     *
     * @throws IOException If there is an error reading data from the underlying stream.
     */
    @Override
    public int read(final byte[] data, final int offset, final int len) throws IOException {
        if (eof && index >= n) {
            return -1;
        }
        for (int i = 0; i < len; i++) {
            if (index < n) {
                data[i + offset] = decoded[index++];
            } else {
                int t = read();
                if (t == -1) {
                    return i;
                }
                data[i + offset] = (byte) t;
            }
        }
        return len;
    }

    /**
     * This will close the underlying stream and release any resources.
     *
     * @throws IOException If there is an error closing the underlying stream.
     */
    @Override
    public void close() throws IOException {
        eof = true;
        super.close();
    }

    /**
     * non supported interface methods.
     *
     * @return False always.
     */
    @Override
    public boolean markSupported() {
        return false;
    }

    /**
     * Unsupported.
     *
     * @param nValue ignored.
     *
     * @return Always zero.
     */
    @Override
    public long skip(long nValue) {
        return 0;
    }

    /**
     * Unsupported.
     *
     * @return Always zero.
     */
    @Override
    public int available() {
        return 0;
    }

    /**
     * Unsupported.
     *
     * @param readlimit ignored.
     */
    @Override
    public void mark(int readlimit) {}

    /**
     * Unsupported.
     *
     * @throws IOException telling that this is an unsupported action.
     */
    @Override
    public void reset() throws IOException {
        throw new IOException("Reset is not supported");
    }
}
