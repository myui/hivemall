/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013-2015
 *   National Institute of Advanced Industrial Science and Technology (AIST)
 *   Registration Number: H25PRO-1520
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
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
