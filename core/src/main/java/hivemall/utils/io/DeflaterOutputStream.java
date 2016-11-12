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
import java.io.OutputStream;
import java.util.zip.Deflater;

/**
 * {@link java.util.zip.DeflaterOutputStream} does not call {@link Deflater#end()} on
 * {@link #close()} when Deflater is specified on the constructor. This causes memory leaking in
 * non-heap.
 */
public final class DeflaterOutputStream extends java.util.zip.DeflaterOutputStream {

    /**
     * Indicates that the stream has been closed.
     */
    private boolean closed = false;

    public DeflaterOutputStream(OutputStream out) {
        super(out);
    }

    public DeflaterOutputStream(OutputStream out, Deflater def) {
        super(out, def);
    }

    public DeflaterOutputStream(OutputStream out, Deflater def, int size) {
        super(out, def, size);
    }

    /**
     * Writes remaining compressed data to the output stream and closes the underlying stream.
     * 
     * @exception IOException if an I/O error has occurred
     */
    @Override
    public void close() throws IOException {
        if (!closed) {
            finish();
            def.end();
            out.close();
            closed = true;
        }
    }

}
