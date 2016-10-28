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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

public abstract class FinishableOutputStreamAdapter extends FinishableOutputStream {

    @Nonnull
    protected final OutputStream _out;

    public FinishableOutputStreamAdapter(@CheckForNull OutputStream out) {
        this._out = Preconditions.checkNotNull(out);
    }

    @Override
    public void write(int b) throws IOException {
        _out.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        _out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        _out.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        _out.flush();
    }

    @Override
    public void close() throws IOException {
        _out.close();
    }

}
