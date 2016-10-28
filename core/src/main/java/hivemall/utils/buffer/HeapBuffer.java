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
package hivemall.utils.buffer;

import hivemall.utils.lang.NumberUtils;
import hivemall.utils.lang.Preconditions;
import hivemall.utils.lang.Primitives;
import hivemall.utils.lang.SizeOf;
import hivemall.utils.lang.UnsafeUtils;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import sun.misc.Unsafe;

@SuppressWarnings("restriction")
@NotThreadSafe
public final class HeapBuffer {
    /** 4 * 1024 * 1024 = 4M entries */
    public static final int DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024;
    /** 4 bytes (int) * 4M = 16 MiB */
    public static final int DEFAULT_CHUNK_BYTES = SizeOf.INT * DEFAULT_CHUNK_SIZE;
    /** 16 MiB * 8 Chunks = 128 MiB */
    public static final int DEFAULT_NUM_CHUNKS = 8;

    @Nonnull
    private final Unsafe _UNSAFE;

    private final int _chunkSize;
    private final int _chunkBytes;
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

    public HeapBuffer() {
        this(DEFAULT_CHUNK_SIZE);
    }

    public HeapBuffer(int chunkSize) {
        this(chunkSize, DEFAULT_NUM_CHUNKS);
    }

    public int getChunkSize() {
        return _chunkBytes;
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

    public long consumedBytes() {
        return _chunkBytes * _initializedChunks;
    }

    public int getNumInitializedChunks() {
        return _initializedChunks;
    }

    public int getNumChunks() {
        return _chunks.length;
    }

    public long position() {
        return _position;
    }

    public int getNumAllocated() {
        return _numAllocated;
    }

    public long getAllocatedBytes() {
        return _allocatedBytes;
    }

    public long getSkippedBytes() {
        return _skippedBytes;
    }
}
