/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
 * Copyright (C) 2013-2015 National Institute of Advanced Industrial Science and Technology (AIST)
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
package hivemall.utils.io;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public final class NioFixedSegment extends NioSegment {

    private final int recordLength;

    public NioFixedSegment(@Nonnull File file, int recordLength) {
        this(file, recordLength, false);
    }

    public NioFixedSegment(@Nonnull File file, int recordLength, boolean readOnly) {
        super(file, readOnly);
        this.recordLength = recordLength;
    }

    public int read(final long idx, final ByteBuffer buf) throws IOException {
        long ptr = toPhysicalAddr(idx);
        int readBytes = NIOUtils.read(channel, buf, ptr);
        return readBytes == 0 ? 0 : readBytes / recordLength;
    }

    public long write(final long idx, final ByteBuffer buf) throws IOException {
        long ptr = toPhysicalAddr(idx);
        NIOUtils.writeFully(channel, buf, ptr);
        return ptr;
    }

    private long toPhysicalAddr(final long logicalAddr) {
        return logicalAddr * recordLength;
    }
}
