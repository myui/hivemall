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

public final class NioStatefullSegment extends NioSegment {

    private long curPos;

    public NioStatefullSegment(File file) {
        this(file, false);
    }

    public NioStatefullSegment(File file, boolean readOnly) {
        super(file, readOnly);
        this.curPos = 0L;
    }

    public long getCurrentPosition() {
        return curPos;
    }

    public void setCurrentPosition(long pos) {
        this.curPos = pos;
    }

    public int read(@Nonnull ByteBuffer buf) throws IOException {
        int bytes = super.read(curPos, buf);
        this.curPos += bytes;
        return bytes;
    }

    public int write(@Nonnull ByteBuffer buf) throws IOException {
        int bytes = super.write(curPos, buf);
        this.curPos += bytes;
        return bytes;
    }

    @Override
    public int read(long filePos, @Nonnull ByteBuffer buf) throws IOException {
        int bytes = super.read(filePos, buf);
        this.curPos += bytes;
        return bytes;
    }

    @Override
    public int write(long filePos, @Nonnull ByteBuffer buf) throws IOException {
        int bytes = super.write(filePos, buf);
        this.curPos += bytes;
        return bytes;
    }

}
