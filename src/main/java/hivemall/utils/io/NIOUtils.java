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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

public final class NIOUtils {

    private NIOUtils() {}

    /**
     * Read until dst buffer is filled or src channel is reached end.
     * 
     * @return The number of bytes read, 0 or more
     */
    public static int read(@Nonnull final FileChannel src, @Nonnull final ByteBuffer dst, @Nonnegative final long position)
            throws IOException {
        int count = 0;
        long offset = position;
        while(dst.remaining() > 0) {
            int n = src.read(dst, offset);
            if(n == -1) {
                break;
            }
            offset += n;
            count += n;
        }
        return count;
    }

    public static void readFully(final FileChannel src, final ByteBuffer dst, final long position)
            throws IOException {
        while(dst.remaining() > 0) {
            if(-1 == src.read(dst, position + dst.position())) {
                throw new EOFException();
            }
        }
    }

    public static void writeFully(@Nonnull final FileChannel dst, @Nonnull final ByteBuffer src, @Nonnegative final long position)
            throws IOException {
        while(src.remaining() > 0) {
            dst.write(src, position + src.position());
        }
    }

}
