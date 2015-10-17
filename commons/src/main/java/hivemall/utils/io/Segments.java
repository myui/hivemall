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

