/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
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
package hivemall.utils.buffer;

import hivemall.utils.lang.NumberUtils;
import hivemall.utils.lang.Preconditions;
import hivemall.utils.lang.Primitives;
import hivemall.utils.lang.SizeOf;
import hivemall.utils.lang.UnsafeUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import sun.misc.Unsafe;

@SuppressWarnings("restriction")
@NotThreadSafe
public final class HeapBuffer implements Externalizable {
    /** 4 * 1024 * 1024 entries * 4 B (int) = 16 MiB */
    public static final int DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024;
    /** 16 MiB * 8 Chunks = 128 MiB */
    public static final int DEFAULT_NUM_CHUNKS = 8;

    @Nonnull
    private final Unsafe _UNSAFE;

    private/* final */int _chunkSize;
    private/* final */int _chunkBytes;
    @Nonnull
    private int[][] _chunks;
    /** the number of chunks created */
    private int _initializedChunks;

    /** physical address pointer */
    private long _position;

    //-----------------------------
    // buffer stats
    /** The number of allocation */
    private int _numAllocated;
    /** Total allocated bytes */
    private long _allocatedBytes;
    /** Total skipped bytes */
    private long _skippedBytes;

    //-----------------------------

    /**
     * Constructor for Externalizable. Should not be called otherwise.
     */
    public HeapBuffer() {
        this._UNSAFE = UnsafeUtils.getUnsafe();
    }// for Externalizable

    public HeapBuffer(int chunkSize) {
        this(chunkSize, DEFAULT_NUM_CHUNKS);
    }

    public HeapBuffer(int chunkSize, int initNumChunks) {
        this._UNSAFE = UnsafeUtils.getUnsafe();
        this._chunkSize = chunkSize;
        this._chunkBytes = SizeOf.INT * chunkSize;
        this._chunks = new int[initNumChunks][];
        this._initializedChunks = 0;
        this._position = 0L;
        this._numAllocated = 0;
        this._allocatedBytes = 0L;
        this._skippedBytes = 0L;
    }

    @Nonnull
    public static HeapBuffer newInstance() {
        return new HeapBuffer(DEFAULT_CHUNK_SIZE);
    }

    public void init(int chunkSize, long position, @Nonnull int[][] chunks) {
        this._chunkSize = chunkSize;
        this._chunkBytes = chunkSize * SizeOf.INT;
        this._chunks = chunks;
        this._initializedChunks = chunks.length;
        this._position = position;
    }

    /**
     * @param bytes allocating bytes
     * @return pointer of the allocated object
     */
    public long allocate(final int bytes) {
        Preconditions.checkArgument(bytes > 0, "Failed to allocate bytes : %s", bytes);
        Preconditions.checkArgument(bytes <= _chunkBytes,
            "Cannot allocate memory greater than %s bytes: %s", _chunkBytes, bytes);

        int i = Primitives.castToInt(_position / _chunkBytes);
        final int j = Primitives.castToInt(_position % _chunkBytes);
        if (bytes > (_chunkBytes - j)) {
            // cannot allocate the object in the current chunk
            // so, skip the current chunk
            _skippedBytes += (_chunkBytes - j);
            i++;
            _position = ((long) i) * _chunkBytes;
        }
        grow(i);

        long ptr = _position;
        this._position += bytes;
        this._allocatedBytes += bytes;
        this._numAllocated++;

        return ptr;
    }

    private void grow(final int chunkIndex) {
        if (chunkIndex < _initializedChunks) {
            return; // no need to grow
        }

        int[][] chunks = _chunks;
        if (chunkIndex >= _chunks.length) {
            int newSize = Math.max(chunkIndex + 1, _chunks.length * 2);
            int[][] newChunks = new int[newSize][];
            System.arraycopy(_chunks, 0, newChunks, 0, _chunks.length);
            this._chunks = newChunks;
            chunks = newChunks;
        }
        for (int i = _initializedChunks; i <= chunkIndex; ++i) {
            chunks[i] = new int[_chunkSize];
        }
        this._initializedChunks = chunkIndex + 1;
    }

    private void validatePointer(final long ptr) {
        if (ptr >= _position) {
            throw new IllegalArgumentException("Invalid pointer " + ptr + " does not in range [0,"
                    + _position + ')');
        }
    }

    public byte getByte(final long ptr) {
        validatePointer(ptr);
        int i = Primitives.castToInt(ptr / _chunkBytes);
        int[] chunk = _chunks[i];
        long j = offset(ptr);
        return _UNSAFE.getByte(chunk, j);
    }

    public void putByte(final long ptr, final byte value) {
        validatePointer(ptr);
        int i = Primitives.castToInt(ptr / _chunkBytes);
        int[] chunk = _chunks[i];
        long j = offset(ptr);
        _UNSAFE.putByte(chunk, j, value);
    }

    public int getInt(final long ptr) {
        validatePointer(ptr);
        int i = Primitives.castToInt(ptr / _chunkBytes);
        int[] chunk = _chunks[i];
        long j = offset(ptr);
        return _UNSAFE.getInt(chunk, j);
    }

    public void putInt(final long ptr, final int value) {
        validatePointer(ptr);
        int i = Primitives.castToInt(ptr / _chunkBytes);
        int[] chunk = _chunks[i];
        long j = offset(ptr);
        _UNSAFE.putInt(chunk, j, value);
    }

    public short getShort(final long ptr) {
        validatePointer(ptr);
        int i = Primitives.castToInt(ptr / _chunkBytes);
        int[] chunk = _chunks[i];
        long j = offset(ptr);
        return _UNSAFE.getShort(chunk, j);
    }

    public void putShort(final long ptr, final short value) {
        validatePointer(ptr);
        int i = Primitives.castToInt(ptr / _chunkBytes);
        int[] chunk = _chunks[i];
        long j = offset(ptr);
        _UNSAFE.putShort(chunk, j, value);
    }

    public char getChar(final long ptr) {
        validatePointer(ptr);
        int i = Primitives.castToInt(ptr / _chunkBytes);
        int[] chunk = _chunks[i];
        long j = offset(ptr);
        return _UNSAFE.getChar(chunk, j);
    }

    public void putChar(final long ptr, final char value) {
        validatePointer(ptr);
        int i = Primitives.castToInt(ptr / _chunkBytes);
        int[] chunk = _chunks[i];
        long j = offset(ptr);
        _UNSAFE.putChar(chunk, j, value);
    }

    public long getLong(final long ptr) {
        int i = Primitives.castToInt(ptr / _chunkBytes);
        int[] chunk = _chunks[i];
        long j = offset(ptr);
        return _UNSAFE.getLong(chunk, j);
    }

    public void putLong(final long ptr, final long value) {
        validatePointer(ptr);
        int i = Primitives.castToInt(ptr / _chunkBytes);
        int[] chunk = _chunks[i];
        long j = offset(ptr);
        _UNSAFE.putLong(chunk, j, value);
    }

    public float getFloat(final long ptr) {
        validatePointer(ptr);
        int i = Primitives.castToInt(ptr / _chunkBytes);
        int[] chunk = _chunks[i];
        long j = offset(ptr);
        return _UNSAFE.getFloat(chunk, j);
    }

    public void putFloat(final long ptr, final float value) {
        validatePointer(ptr);
        int i = Primitives.castToInt(ptr / _chunkBytes);
        int[] chunk = _chunks[i];
        long j = offset(ptr);
        _UNSAFE.putFloat(chunk, j, value);
    }

    public double getDouble(final long ptr) {
        validatePointer(ptr);
        int i = Primitives.castToInt(ptr / _chunkBytes);
        int[] chunk = _chunks[i];
        long j = offset(ptr);
        return _UNSAFE.getDouble(chunk, j);
    }

    public void putDouble(final long ptr, final double value) {
        validatePointer(ptr);
        int i = Primitives.castToInt(ptr / _chunkBytes);
        int[] chunk = _chunks[i];
        long j = offset(ptr);
        _UNSAFE.putDouble(chunk, j, value);
    }

    public void getFloats(final long ptr, @Nonnull final float[] values) {
        validatePointer(ptr);
        final int len = values.length;
        if (len == 0) {
            throw new IllegalArgumentException("Cannot put empty array at " + ptr);
        }

        int chunkIdx = Primitives.castToInt(ptr / _chunkBytes);
        final int[] chunk = _chunks[chunkIdx];
        final long base = offset(ptr);
        for (int i = 0; i < len; i++) {
            long offset = base + SizeOf.FLOAT * i;
            validateOffset(offset);
            values[i] = _UNSAFE.getFloat(chunk, offset);
        }
    }

    public void putFloats(final long ptr, @Nonnull final float[] values) {
        validatePointer(ptr);
        final int len = values.length;
        if (len == 0) {
            throw new IllegalArgumentException("Cannot put empty array at " + ptr);
        }

        int chunkIdx = Primitives.castToInt(ptr / _chunkBytes);
        final int[] chunk = _chunks[chunkIdx];
        final long base = offset(ptr);
        for (int i = 0; i < len; i++) {
            long offset = base + SizeOf.FLOAT * i;
            validateOffset(offset);
            _UNSAFE.putFloat(chunk, offset, values[i]);
        }
    }

    private void validateOffset(final long offset) {
        if (offset >= _chunkBytes) {
            throw new IndexOutOfBoundsException("Invalid offset " + offset + " not in range [0,"
                    + _chunkBytes + ')');
        }
    }

    /**
     * Returns an offset in a chunk
     * 
     * @param ptr physical address
     * @return physical offset in a chunk
     */
    private long offset(final long ptr) {
        long j = ptr % _chunkBytes;
        return Unsafe.ARRAY_INT_BASE_OFFSET + j;
    }

    @Override
    public String toString() {
        return "HeapBuffer [position=" + NumberUtils.formatNumber(_position)
                + ", #allocatedObjects=" + NumberUtils.formatNumber(_numAllocated) + ", #consumed="
                + NumberUtils.prettySize(consumedBytes()) + ", #allocated="
                + NumberUtils.prettySize(_allocatedBytes) + ", #skipped="
                + NumberUtils.prettySize(_skippedBytes) + ", #chunks="
                + NumberUtils.formatNumber(_chunks.length) + ", #initializedChunks="
                + NumberUtils.formatNumber(_initializedChunks) + ", #chunkSize="
                + NumberUtils.formatNumber(_chunkSize) + ", #chunkBytes="
                + NumberUtils.formatNumber(_chunkBytes) + " bytes]";
    }

    long position() {
        return _position;
    }

    public long consumedBytes() {
        return _chunkBytes * _initializedChunks;
    }

    long allocatedBytes() {
        return _allocatedBytes;
    }

    int getNumChunks() {
        return _chunks.length;
    }

    int getNumInitializedChunks() {
        return _initializedChunks;
    }

    int getChunkSize() {
        return _chunkBytes;
    }

    @Override
    public void writeExternal(@Nonnull final ObjectOutput out) throws IOException {
        out.writeInt(_chunkSize);
        out.writeLong(_position);

        final int[][] chunks = _chunks;
        final int numChunks = _initializedChunks;
        out.writeInt(numChunks);
        for (int i = 0; i < numChunks; i++) {
            final int[] chunk = chunks[i];
            if (chunk.length != _chunkSize) {
                throw new IllegalStateException("Illegal chunk size at chunk[" + i + ']');
            }
            for (int j = 0; j < chunk.length; j++) {
                out.writeInt(chunk[j]);
            }
            chunks[i] = null;
        }
        // help GC
        this._chunks = null;
    }

    @Override
    public void readExternal(@Nonnull final ObjectInput in) throws IOException,
            ClassNotFoundException {
        final int chunkSize = in.readInt();
        final long position = in.readLong();
        final int numChunks = in.readInt();
        final int[][] chunks = new int[numChunks][];
        for (int i = 0; i < numChunks; i++) {
            final int[] chunk = new int[chunkSize];
            for (int j = 0; j < chunkSize; j++) {
                chunk[j] = in.readInt();
            }
            chunks[i] = chunk;
        }

        init(chunkSize, position, chunks);
    }

}
