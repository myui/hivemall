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
package hivemall.utils.codec;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * This class represents an ASCII85 output stream.
 * This class is based on the implementation in Apache PDFBox.
 * 
 * @author Ben Litchfield
 */
public final class ASCII85OutputStream extends FilterOutputStream {
    private static final long a85p2 = 85L * 85L;
    private static final long a85p3 = 85L * 85L * 85L;
    private static final long a85p4 = 85L * 85L * 85L * 85L;
    private static final byte TERMINATOR = '~';
    private static final byte OFFSET = '!';
    private static final byte Z = 'z';

    private int count;
    private byte[] indata;
    private byte[] outdata;
    private boolean flushed;

    /**
     * Constructor.
     *
     * @param out The output stream to write to.
     */
    public ASCII85OutputStream(OutputStream out) {
        super(out);
        count = 0;
        indata = new byte[4];
        outdata = new byte[5];
        flushed = true;
    }

    /**
     * This will transform the next four ascii bytes.
     */
    private void transformASCII85() {
        long word = ((((indata[0] << 8) | (indata[1] & 0xFF)) << 16) | ((indata[2] & 0xFF) << 8) | (indata[3] & 0xFF)) & 0xFFFFFFFFL;

        if (word == 0) {
            outdata[0] = Z;
            outdata[1] = 0;
            return;
        }

        long x = word / a85p4;
        outdata[0] = (byte) (x + OFFSET);
        word -= x * a85p4;

        x = word / a85p3;
        outdata[1] = (byte) (x + OFFSET);
        word -= x * a85p3;

        x = word / a85p2;
        outdata[2] = (byte) (x + OFFSET);
        word -= x * a85p2;

        x = word / 85L;
        outdata[3] = (byte) (x + OFFSET);

        outdata[4] = (byte) ((word % 85L) + OFFSET);
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
            if (outdata[i] == 0) {
                break;
            }
            out.write(outdata[i]);
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
            if (outdata[0] == Z) {
                for (int i = 0; i < 5; i++) // expand 'z',
                {
                    outdata[i] = OFFSET;
                }
            }
            for (int i = 0; i < count + 1; i++) {
                out.write(outdata[i]);
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
        try {
            flush();
            super.close();
        } finally {
            indata = outdata = null;
        }
    }
}
