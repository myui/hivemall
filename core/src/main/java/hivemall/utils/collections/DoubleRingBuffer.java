/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2016 Makoto YUI
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

import hivemall.utils.lang.Preconditions;

import java.util.Iterator;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public final class DoubleRingBuffer implements Iterable<Double> {

    private final double[] ring;
    private final int capacity;

    private int size;
    private int head;

    public DoubleRingBuffer(int capacity) {
        this.ring = new double[capacity];
        this.capacity = capacity;
        this.size = 0;
        this.head = 0;
    }

    @Nonnull
    public double[] getRing() {
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

    public DoubleRingBuffer add(final double x) {
        ring[head] = x;
        head = (head + 1) % capacity;
        if (size != capacity) {
            size++;
        }
        return this;
    }

    public void toArray(@Nonnull final double[] dst) {
        toArray(dst, true);
    }

    public void toArray(@Nonnull final double[] dst, final boolean fifo) {
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
                dst[i] = 0;
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
    public Iterator<Double> iterator() {
        return new Itor();
    }

    private final class Itor implements Iterator<Double> {

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
        public Double next() {
            Double d = ring[curr];
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
