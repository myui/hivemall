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

import java.util.Comparator;
import java.util.PriorityQueue;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class BoundedPriorityQueue<E> {

    @Nonnegative
    private final int maxSize;
    @Nonnull
    private final Comparator<E> comparator;
    @Nonnull
    private final PriorityQueue<E> queue;

    public BoundedPriorityQueue(int size, @Nonnull Comparator<E> comparator) {
        if(size < 1) {
            throw new IllegalArgumentException("Illegal queue size: " + size);
        }
        if(comparator == null) {
            throw new IllegalArgumentException("comparator should not be null");
        }
        this.maxSize = size;
        this.comparator = comparator;
        this.queue = new PriorityQueue<E>(size + 10, comparator);
    }

    public boolean offer(@Nonnull E e) {
        if(e == null) {
            throw new IllegalArgumentException("Null argument is not permitted");
        }
        final int numElem = queue.size();
        if(numElem >= maxSize) {
            E smallest = queue.peek();
            final int cmp = comparator.compare(e, smallest);
            if(cmp < 0) {
                return false;
            }
            queue.poll();
        }
        queue.offer(e);
        return true;
    }

    @Nullable
    public E poll() {
        return queue.poll();
    }

    @Nullable
    public E peek() {
        return queue.peek();
    }

    public int size() {
        return queue.size();
    }

    public void clear() {
        queue.clear();
    }

}
