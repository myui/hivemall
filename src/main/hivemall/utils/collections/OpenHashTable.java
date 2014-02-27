/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013
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
package hivemall.utils.collections;


import hivemall.utils.math.Primes;

import java.util.Arrays;
import java.util.HashMap;

/**
 * An open-addressing hash table that requires less memory to {@link HashMap}.
 */
public class OpenHashTable<K, V> {

    public static final float DEFAULT_LOAD_FACTOR = 0.7f;
    public static final float DEFAULT_GROW_FACTOR = 2.0f;

    protected static final byte FREE = 0;
    protected static final byte FULL = 1;
    protected static final byte REMOVED = 2;

    protected final float _loadFactor;
    protected final float _growFactor;

    protected int _used = 0;
    protected int _threshold;

    protected K[] _keys;
    protected V[] _values;
    protected byte[] _states;

    public OpenHashTable(int size) {
        this(size, DEFAULT_LOAD_FACTOR, DEFAULT_GROW_FACTOR);
    }

    @SuppressWarnings("unchecked")
    public OpenHashTable(int size, float loadFactor, float growFactor) {
        if(size < 1) {
            throw new IllegalArgumentException();
        }
        this._loadFactor = loadFactor;
        this._growFactor = growFactor;
        int actualSize = Primes.findLeastPrimeNumber(size);
        this._keys = (K[]) new Object[actualSize];
        this._values = (V[]) new Object[actualSize];
        this._states = new byte[actualSize];
        this._threshold = Math.round(actualSize * _loadFactor);
    }

    public boolean containsKey(K key) {
        return findKey(key) >= 0;
    }

    public V get(K key) {
        final int i = findKey(key);
        if(i < 0) {
            return null;
        }
        return _values[i];
    }

    public V put(K key, V value) {
        int hash = keyHash(key);
        int keyLength = _keys.length;
        int keyIdx = hash % keyLength;

        boolean expanded = preAddEntry(keyIdx);
        if(expanded) {
            keyLength = _keys.length;
            keyIdx = hash % keyLength;
        }

        K[] keys = _keys;
        V[] values = _values;
        byte[] states = _states;

        if(states[keyIdx] == FULL) {
            if(equals(keys[keyIdx], key)) {
                V old = values[keyIdx];
                values[keyIdx] = value;
                return old;
            }
            // try second hash
            int decr = 1 + (hash % (keyLength - 2));
            for(;;) {
                keyIdx -= decr;
                if(keyIdx < 0) {
                    keyIdx += keyLength;
                }
                if(isFree(keyIdx, key)) {
                    break;
                }
                if(states[keyIdx] == FULL && equals(keys[keyIdx], key)) {
                    V old = values[keyIdx];
                    values[keyIdx] = value;
                    return old;
                }
            }
        }
        keys[keyIdx] = key;
        values[keyIdx] = value;
        states[keyIdx] = FULL;
        ++_used;
        return null;
    }

    private static boolean equals(final Object k1, final Object k2) {
        return k1 == k2 || k1.equals(k2);
    }

    /** Return weather the required slot is free for new entry */
    protected boolean isFree(int index, K key) {
        byte stat = _states[index];
        if(stat == FREE) {
            return true;
        }
        if(stat == REMOVED && equals(_keys[index], key)) {
            return true;
        }
        return false;
    }

    /** @return expanded or not */
    protected boolean preAddEntry(int index) {
        if(_used >= _threshold) {// filled enough
            int newCapacity = Math.round(_keys.length * _growFactor);
            ensureCapacity(newCapacity);
            return true;
        }
        return false;
    }

    protected int findKey(K key) {
        K[] keys = _keys;
        byte[] states = _states;
        int keyLength = keys.length;

        int hash = keyHash(key);
        int keyIdx = hash % keyLength;
        if(states[keyIdx] != FREE) {
            if(states[keyIdx] == FULL && equals(keys[keyIdx], key)) {
                return keyIdx;
            }
            // try second hash
            int decr = 1 + (hash % (keyLength - 2));
            for(;;) {
                keyIdx -= decr;
                if(keyIdx < 0) {
                    keyIdx += keyLength;
                }
                if(isFree(keyIdx, key)) {
                    return -1;
                }
                if(states[keyIdx] == FULL && equals(keys[keyIdx], key)) {
                    return keyIdx;
                }
            }
        }
        return -1;
    }

    public V remove(K key) {
        K[] keys = _keys;
        V[] values = _values;
        byte[] states = _states;
        int keyLength = keys.length;

        int hash = keyHash(key);
        int keyIdx = hash % keyLength;
        if(states[keyIdx] != FREE) {
            if(states[keyIdx] == FULL && equals(keys[keyIdx], key)) {
                V old = values[keyIdx];
                states[keyIdx] = REMOVED;
                --_used;
                return old;
            }
            //  second hash
            int decr = 1 + (hash % (keyLength - 2));
            for(;;) {
                keyIdx -= decr;
                if(keyIdx < 0) {
                    keyIdx += keyLength;
                }
                if(states[keyIdx] == FREE) {
                    return null;
                }
                if(states[keyIdx] == FULL && equals(keys[keyIdx], key)) {
                    V old = values[keyIdx];
                    states[keyIdx] = REMOVED;
                    --_used;
                    return old;
                }
            }
        }
        return null;
    }

    public int size() {
        return _used;
    }

    public void clear() {
        Arrays.fill(_states, FREE);
        this._used = 0;
    }

    public IMapIterator<K, V> entries() {
        return new MapIterator();
    }

    @Override
    public String toString() {
        int len = size() * 10 + 2;
        final StringBuilder buf = new StringBuilder(len);
        buf.append('{');
        final IMapIterator<K, V> i = entries();
        while(i.next() != -1) {
            String key = i.getKey().toString();
            buf.append(key);
            buf.append('=');
            buf.append(i.getValue());
            if(i.hasNext()) {
                buf.append(',');
            }
        }
        buf.append('}');
        return buf.toString();
    }

    protected void ensureCapacity(int newCapacity) {
        if(_used < _threshold) {
            throw new IllegalStateException("used: " + _used + ", threshold: " + _threshold);
        }
        int prime = Primes.findLeastPrimeNumber(newCapacity);
        rehash(prime);
        this._threshold = Math.round(prime * _loadFactor);
    }

    @SuppressWarnings("unchecked")
    private void rehash(int newCapacity) {
        int oldCapacity = _keys.length;
        if(newCapacity <= oldCapacity) {
            throw new IllegalArgumentException("new: " + newCapacity + ", old: " + oldCapacity);
        }
        final K[] newkeys = (K[]) new Object[newCapacity];
        final V[] newValues = (V[]) new Object[newCapacity];
        final byte[] newStates = new byte[newCapacity];
        int used = 0;
        for(int i = 0; i < oldCapacity; i++) {
            if(_states[i] == FULL) {
                used++;
                K k = _keys[i];
                V v = _values[i];
                int hash = keyHash(k);
                int keyIdx = hash % newCapacity;
                if(newStates[keyIdx] == FULL) {// second hashing
                    int decr = 1 + (hash % (newCapacity - 2));
                    while(newStates[keyIdx] != FREE) {
                        keyIdx -= decr;
                        if(keyIdx < 0) {
                            keyIdx += newCapacity;
                        }
                    }
                }
                newStates[keyIdx] = FULL;
                newkeys[keyIdx] = k;
                newValues[keyIdx] = v;
            }
        }
        this._keys = newkeys;
        this._values = newValues;
        this._states = newStates;
        this._used = used;
    }

    private static int keyHash(final Object key) {
        int hash = key.hashCode();
        return ((int) (hash ^ (hash >>> 32))) & 0x7fffffff;
    }

    public interface IMapIterator<K, V> {

        public boolean hasNext();

        /**
         * @return -1 if not found
         */
        public int next();

        public K getKey();

        public V getValue();

    }

    private final class MapIterator implements IMapIterator<K, V> {

        int nextEntry;
        int lastEntry = -1;

        MapIterator() {
            this.nextEntry = nextEntry(0);
        }

        /** find the index of next full entry */
        int nextEntry(int index) {
            while(index < _keys.length && _states[index] != FULL) {
                index++;
            }
            return index;
        }

        public boolean hasNext() {
            return nextEntry < _keys.length;
        }

        public int next() {
            if(!hasNext()) {
                return -1;
            }
            int curEntry = nextEntry;
            this.lastEntry = nextEntry;
            this.nextEntry = nextEntry(nextEntry + 1);
            return curEntry;
        }

        public K getKey() {
            if(lastEntry == -1) {
                throw new IllegalStateException();
            }
            return _keys[lastEntry];
        }

        public V getValue() {
            if(lastEntry == -1) {
                throw new IllegalStateException();
            }
            return _values[lastEntry];
        }
    }
}
