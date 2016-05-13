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
package hivemall.utils.collections;

import hivemall.utils.lang.Primitives;

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;

import javax.annotation.Nonnull;

public final class DoubleArray3D {
    private static final int DEFAULT_SIZE = 100 * 100 * 10; // feature * field * factor

    private final boolean direct;

    @Nonnull
    private DoubleBuffer buffer;
    private int capacity;

    private int size;
    // number of array in each dimension
    private int n1, n2, n3;
    // pointer to each dimension
    private int p1, p2;

    private boolean sanityCheck;

    public DoubleArray3D() {
        this(DEFAULT_SIZE, true);
    }

    public DoubleArray3D(int initSize, boolean direct) {
        this.direct = direct;
        this.buffer = allocate(direct, initSize);
        this.capacity = initSize;
        this.size = -1;
        this.sanityCheck = true;
    }

    public DoubleArray3D(int dim1, int dim2, int dim3) {
        this.direct = true;
        this.capacity = -1;
        configure(dim1, dim2, dim3);
        this.sanityCheck = true;
    }

    public void setSanityCheck(boolean enable) {
        this.sanityCheck = enable;
    }

    public void configure(final int dim1, final int dim2, final int dim3) {
        int requiredSize = cardinarity(dim1, dim2, dim3);
        if (requiredSize > capacity) {
            this.buffer = allocate(direct, requiredSize);
            this.capacity = requiredSize;
        }
        this.size = requiredSize;
        this.n1 = dim1;
        this.n2 = dim2;
        this.n3 = dim3;
        this.p1 = n2 * n3;
        this.p2 = n3;
    }

    public void clear() {
        buffer.clear();
        this.size = -1;
    }

    public int getSize() {
        return size;
    }

    int getCapacity() {
        return capacity;
    }

    public double get(final int i, final int j, final int k) {
        int idx = idx(i, j, k);
        return buffer.get(idx);
    }

    public void set(final int i, final int j, final int k, final double val) {
        int idx = idx(i, j, k);
        buffer.put(idx, val);
    }

    private int idx(final int i, final int j, final int k) {
        if (sanityCheck == false) {
            return i * p1 + j * p2 + k;
        }

        if (size == -1) {
            throw new IllegalStateException("Double3DArray#configure() is not called");
        }
        if (i >= n1 || i < 0) {
            throw new ArrayIndexOutOfBoundsException("Index '" + i
                    + "' out of bounds for 1st dimension of size " + n1);
        }
        if (j >= n2 || j < 0) {
            throw new ArrayIndexOutOfBoundsException("Index '" + j
                    + "' out of bounds for 2nd dimension of size " + n2);
        }
        if (k >= n3 || k < 0) {
            throw new ArrayIndexOutOfBoundsException("Index '" + k
                    + "' out of bounds for 3rd dimension of size " + n3);
        }
        final int idx = i * p1 + j * p2 + k;
        if (idx >= size) {
            throw new IndexOutOfBoundsException("Computed internal index '" + idx
                    + "' exceeds buffer size '" + size + "' where i=" + i + ", j=" + j + ", k=" + k);
        }
        return idx;
    }

    private static int cardinarity(final int dim1, final int dim2, final int dim3) {
        if (dim1 <= 0 || dim2 <= 0 || dim3 <= 0) {
            throw new IllegalArgumentException("Detected negative dimension size. dim1=" + dim1
                    + ", dim2=" + dim2 + ", dim3=" + dim3);
        }
        return dim1 * dim2 * dim3;
    }

    @Nonnull
    private static DoubleBuffer allocate(final boolean direct, final int size) {
        int bytes = size * Primitives.DOUBLE_BYTES;
        ByteBuffer buf = direct ? ByteBuffer.allocateDirect(bytes) : ByteBuffer.allocate(bytes);
        return buf.asDoubleBuffer();
    }
}
