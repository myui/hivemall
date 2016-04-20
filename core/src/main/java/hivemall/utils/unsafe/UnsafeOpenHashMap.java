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
package hivemall.utils.unsafe;

import hivemall.utils.math.MathUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

// NOTE: This hashmap points outside data
public final class UnsafeOpenHashMap<K> {
    private K[] keys;

    private int capacity;

    // total number of entries in this table
    private int size;
    // number of bits for the value table (eg. 8 bits = 256 entries)
    private int bits;
    // the number of bits in each sweep zone.
    private int sweepbits;
    // the size of a sweep (2 to the power of sweepbits)
    private int sweep;
    // the sweepmask used to create sweep zone offsets
    private int sweepmask;

    public UnsafeOpenHashMap() {}

    // If space exists for a given `key`, it returns the offset,
    // otherwise returns -1.
    public int put(K key) {
        if(key == null) {
            throw new NullPointerException(this.getClass().getName() + " key");
        }

        while (true) {
            int off = getBucketOffset(key);
            int end = off + sweep;
            for(; off < end; off++) {
                K searchKey = keys[off];
                if(searchKey == null) {
                    // insert
                    keys[off] = key;
                    size++;
                    return off;
                }
            }
            return -1;
        }
    }

    public int get(Object key) {
        int off = getBucketOffset(key);
        int end = sweep + off;
        for(; off < end; off++) {
            if(keys[off] != null && compare(keys[off], key)) {
                return off;
            }
        }
        return -1;
    }

    public int remove(Object key) {
        int off = getBucketOffset(key);
        int end = sweep + off;
        for(; off < end; off++) {
            if(keys[off] != null && compare(keys[off], key)) {
                keys[off] = null;
                size--;
                return off;
            }
        }
        return -1;
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public boolean containsKey(Object key) {
        return get(key) != -1;
    }

    public void clear() {
        Arrays.fill(keys, null);
        size = 0;
    }

    public Set<K> keySet() {
        Set<K> set = new HashSet<K>();
        for(K key : keys) {
            if(key != null) {
                set.add(key);
            }
        }
        return set;
    }

    @SuppressWarnings("unchecked")
    public int resize(int size) {
        this.bits = MathUtils.bitsRequired(size < 256 ? 256 : size);
        this.sweepbits = bits / 4;
        this.sweep = MathUtils.powerOf(2, sweepbits) * 4;
        this.sweepmask = MathUtils.bitMask(bits - this.sweepbits) << sweepbits;
        this.capacity = MathUtils.powerOf(2, bits) + sweep;
        this.size = 0;
        return this.capacity;
    }

    public void reset(K[] keys) {
        assert(this.capacity == keys.length);
        this.keys = keys;
    }

    private int getBucketOffset(Object key) {
        return (key.hashCode() << this.sweepbits) & this.sweepmask;
    }

    private static boolean compare(final Object v1, final Object v2) {
        return v1 == v2 || v1.equals(v2);
    }
}
