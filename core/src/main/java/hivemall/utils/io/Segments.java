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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.Nonnull;

public interface Segments extends Closeable {

    @Nonnull
    public File getFile();

    /**
     * @return The number of read bytes
     */
    public int read(long filePos, @Nonnull ByteBuffer buf) throws IOException;

    /**
     * @return The number of bytes written
     */
    public int write(long filePos, @Nonnull ByteBuffer buf) throws IOException;

    public void flush() throws IOException;

    public void close(boolean deleteFile) throws IOException;

}
