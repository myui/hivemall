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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class NioSegment implements Segments {

    @Nonnull
    private final File file;
    @Nonnull
    private final RandomAccessFile raf;
    @Nonnull
    protected final FileChannel channel;

    public NioSegment(@Nonnull File file) {
        this(file, false);
    }

    public NioSegment(@Nonnull File file, boolean readOnly) {
        this.file = file;
        final RandomAccessFile raf;
        try {
            raf = new RandomAccessFile(file, readOnly ? "r" : "rw");
        } catch (FileNotFoundException e) {
            throw new IllegalStateException("File not found: " + file.getAbsolutePath(), e);
        }
        this.raf = raf;
        this.channel = raf.getChannel();
    }

    @Override
    public final File getFile() {
        return file;
    }

    @Override
    public int read(long filePos, @Nonnull ByteBuffer buf) throws IOException {
        return NIOUtils.read(channel, buf, filePos);
    }

    @Override
    public int write(final long filePos, @Nonnull final ByteBuffer buf) throws IOException {
        return NIOUtils.writeFully(channel, buf, filePos);
    }

    @Override
    public final void close() throws IOException {
        close(false);
    }

    @Override
    public final void close(boolean deleteFile) throws IOException {
        try {
            channel.close();
            raf.close();
        } finally {
            if (deleteFile) {
                if (file.exists()) {
                    file.delete();
                }
            }
        }
    }

    @Override
    public final void flush() throws IOException {
        channel.force(true);
    }

}
