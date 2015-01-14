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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.Nonnull;

public interface Segments extends Closeable {

    /**
     * @return The file position of the written record
     */
    public long write(long idx, @Nonnull byte[] b) throws IOException;

    /**
     * @return The file position of the written record
     */
    public long write(long idx, @Nonnull ByteBuffer buf) throws IOException;

    @Nonnull
    public byte[] read(long idx) throws IOException;

    /**    
     * @return The number of read records
     */
    public int read(long idx, @Nonnull ByteBuffer buf) throws IOException;

    @Nonnull
    public byte[][] readv(@Nonnull long[] idx) throws IOException;

    /**    
     * @return The number of read bytes
     */
    public int directRead(long filePos, @Nonnull ByteBuffer buf) throws IOException;

    public void flush() throws IOException;

    @Nonnull
    public File getFile();

    public void close(boolean deleteFile) throws IOException;

}