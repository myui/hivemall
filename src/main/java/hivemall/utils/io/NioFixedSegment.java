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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public final class NioFixedSegment implements Segments {

    @Nonnull
    private final File file;
    private final int recordLength;
    @Nonnull
    private final RandomAccessFile raf;
    @Nonnull
    private final FileChannel channel;

    public NioFixedSegment(@Nonnull File file, int recordLength) {
        this(file, recordLength, false);
    }

    public NioFixedSegment(@Nonnull File file, int recordLength, boolean readOnly) {
        this.file = file;
        this.recordLength = recordLength;
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
    public File getFile() {
        return file;
    }

    @Override
    public byte[] read(final long idx) throws IOException {
        long ptr = toPhysicalAddr(idx);
        return directRead(ptr, 1);
    }

    @Override
    public int read(final long idx, final ByteBuffer buf) throws IOException {
        long ptr = toPhysicalAddr(idx);
        int readBytes = NIOUtils.read(channel, buf, ptr);
        return readBytes == 0 ? 0 : readBytes / recordLength;
    }

    @Override
    public int directRead(long filePos, @Nonnull ByteBuffer buf) throws IOException {
        return NIOUtils.read(channel, buf, filePos);
    }

    @Override
    public byte[][] readv(final long[] idx) throws IOException {
        final int len = idx.length;
        final byte[][] pages = new byte[len][];
        for(int i = 0; i < len; i++) {
            pages[i] = read(idx[i]);
        }
        return pages;
    }

    private byte[] directRead(final long addr, final int numBlocks) throws IOException {
        byte[] b = new byte[recordLength * numBlocks];
        ByteBuffer buf = ByteBuffer.wrap(b);
        NIOUtils.readFully(channel, buf, addr);
        return b;
    }

    @Override
    public long write(final long idx, final byte[] b) throws IOException {
        checkRecordLength(b);
        long ptr = toPhysicalAddr(idx);
        ByteBuffer writeBuf = ByteBuffer.wrap(b);
        NIOUtils.writeFully(channel, writeBuf, ptr);
        return ptr;
    }

    @Override
    public long write(final long idx, final ByteBuffer buf) throws IOException {
        long ptr = toPhysicalAddr(idx);
        NIOUtils.writeFully(channel, buf, ptr);
        return ptr;
    }

    @Override
    public void close() throws IOException {
        close(false);
    }

    @Override
    public void close(boolean deleteFile) throws IOException {
        channel.close();
        raf.close();
        if(deleteFile) {
            if(file.exists()) {
                file.delete();
            }
        }
    }

    @Override
    public void flush() throws IOException {
        channel.force(true);
    }

    private long toPhysicalAddr(final long logicalAddr) {
        return logicalAddr * recordLength;
    }

    private void checkRecordLength(final byte[] b) {
        if(b.length != recordLength) {
            throw new IllegalArgumentException("Invalid Record length: " + b.length
                    + ", expected: " + recordLength);
        }
    }
}
