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
package hivemall.utils.collections;

import hivemall.utils.lang.Preconditions;

import java.util.Iterator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public final class RingBuffer<T> implements Iterable<T> {

    private final T[] ring;
    private final int capacity;

    private int size;
    private int head;

    @SuppressWarnings("unchecked")
    public RingBuffer(int capacity) {
        this.ring = (T[]) new Object[capacity];
        this.capacity = capacity;
        this.size = 0;
        this.head = 0;
    }

    @Nonnull
    public T[] getRing() {
        return ring;
    }

    public int capacity() {
        return capacity;
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public boolean isFull() {
        return size == capacity;
    }

    public RingBuffer<T> add(final T x) {
        ring[head] = x;
        head = (head + 1) % capacity;
        if (size != capacity) {
            size++;
        }
        return this;
    }

    @Nullable
    public T head() {
        return ring[head];
    }

    public void toArray(@Nonnull final T[] dst) {
        toArray(dst, true);
    }

    public void toArray(@Nonnull final T[] dst, final boolean fifo) {
        Preconditions.checkArgument(dst.length == capacity);

        if (fifo) {
            int curr = isFull() ? head : 0;
            for (int i = 0; i < capacity; i++) {
                dst[i] = ring[curr];
                curr = (curr + 1) % capacity;
            }
        } else {
            int curr = isFull() ? head : 0;
            for (int i = 1; i <= size; i++) {
                dst[size - i] = ring[curr];
                curr = (curr + 1) % capacity;
            }
            for (int i = size; i < capacity; i++) {
                dst[i] = null;
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder(128);
        buf.append('{');
        int curr = isFull() ? head : 0;
        for (int i = 0; i < capacity; i++) {
            if (i != 0) {
                buf.append(',');
            }
            buf.append(ring[curr]);
            curr = (curr + 1) % capacity;
        }
        buf.append('}');
        return buf.toString();
    }

    @Override
    public Iterator<T> iterator() {
        return new Itor();
    }

    private final class Itor implements Iterator<T> {

        private int curr;
        private int i;

        Itor() {
            this.curr = isFull() ? head : 0;
            this.i = 0;
        }

        @Override
        public boolean hasNext() {
            return i < capacity;
        }

        @Override
        public T next() {
            T d = ring[curr];
            curr = (curr + 1) % capacity;
            i++;
            return d;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

}
