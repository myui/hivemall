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

import hivemall.utils.math.Primes;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.HashMap;

import javax.annotation.Nonnull;

/**
 * An open-addressing hash table with double-hashing that requires less memory to {@link HashMap}.
 */
public final class IntOpenHashTable<V> implements Externalizable {

    public static final float DEFAULT_LOAD_FACTOR = 0.7f;
    public static final float DEFAULT_GROW_FACTOR = 2.0f;

    public static final byte FREE = 0;
    public static final byte FULL = 1;
    public static final byte REMOVED = 2;

    protected/* final */float _loadFactor;
    protected/* final */float _growFactor;

    protected int _used = 0;
    protected int _threshold;

    protected int[] _keys;
    protected V[] _values;
    protected byte[] _states;

    public IntOpenHashTable() {} // for Externalizable

    public IntOpenHashTable(int size) {
        this(size, DEFAULT_LOAD_FACTOR, DEFAULT_GROW_FACTOR);
    }

    @SuppressWarnings("unchecked")
    public IntOpenHashTable(int size, float loadFactor, float growFactor) {
        if (size < 1) {
            throw new IllegalArgumentException();
        }
        this._loadFactor = loadFactor;
        this._growFactor = growFactor;
        int actualSize = Primes.findLeastPrimeNumber(size);
        this._keys = new int[actualSize];
        this._values = (V[]) new Object[actualSize];
        this._states = new byte[actualSize];
        this._threshold = Math.round(actualSize * _loadFactor);
    }

    public IntOpenHashTable(@Nonnull int[] keys, @Nonnull V[] values, @Nonnull byte[] states,
            int used) {
        this._used = used;
        this._threshold = keys.length;
        this._keys = keys;
        this._values = values;
        this._states = states;
    }

    public int[] getKeys() {
        return _keys;
    }

    public Object[] getValues() {
        return _values;
    }

    public byte[] getStates() {
        return _states;
    }

    public boolean containsKey(final int key) {
        return findKey(key) >= 0;
    }

    public V get(final int key) {
        final int i = findKey(key);
        if (i < 0) {
            return null;
        }
        return _values[i];
    }

    public V put(final int key, final V value) {
        final int hash = keyHash(key);
        int keyLength = _keys.length;
        int keyIdx = hash % keyLength;

        boolean expanded = preAddEntry(keyIdx);
        if (expanded) {
            keyLength = _keys.length;
            keyIdx = hash % keyLength;
        }

        final int[] keys = _keys;
        final V[] values = _values;
        final byte[] states = _states;

        if (states[keyIdx] == FULL) {
            if (keys[keyIdx] == key) {
                V old = values[keyIdx];
                values[keyIdx] = value;
                return old;
            }
            // try second hash
            int decr = 1 + (hash % (keyLength - 2));
            for (;;) {
                keyIdx -= decr;
                if (keyIdx < 0) {
                    keyIdx += keyLength;
                }
                if (isFree(keyIdx, key)) {
                    break;
                }
                if (states[keyIdx] == FULL && keys[keyIdx] == key) {
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


    /** Return weather the required slot is free for new entry */
    protected boolean isFree(int index, int key) {
        byte stat = _states[index];
        if (stat == FREE) {
            return true;
        }
        if (stat == REMOVED && _keys[index] == key) {
            return true;
        }
        return false;
    }

    /** @return expanded or not */
    protected boolean preAddEntry(int index) {
        if ((_used + 1) >= _threshold) {// filled enough
            int newCapacity = Math.round(_keys.length * _growFactor);
            ensureCapacity(newCapacity);
            return true;
        }
        return false;
    }

    protected int findKey(final int key) {
        final int[] keys = _keys;
        final byte[] states = _states;
        final int keyLength = keys.length;

        final int hash = keyHash(key);
        int keyIdx = hash % keyLength;
        if (states[keyIdx] != FREE) {
            if (states[keyIdx] == FULL && keys[keyIdx] == key) {
                return keyIdx;
            }
            // try second hash
            int decr = 1 + (hash % (keyLength - 2));
            for (;;) {
                keyIdx -= decr;
                if (keyIdx < 0) {
                    keyIdx += keyLength;
                }
                if (isFree(keyIdx, key)) {
                    return -1;
                }
                if (states[keyIdx] == FULL && keys[keyIdx] == key) {
                    return keyIdx;
                }
            }
        }
        return -1;
    }

    public V remove(final int key) {
        final int[] keys = _keys;
        final V[] values = _values;
        final byte[] states = _states;
        final int keyLength = keys.length;

        final int hash = keyHash(key);
        int keyIdx = hash % keyLength;
        if (states[keyIdx] != FREE) {
            if (states[keyIdx] == FULL && keys[keyIdx] == key) {
                V old = values[keyIdx];
                states[keyIdx] = REMOVED;
                --_used;
                return old;
            }
            //  second hash
            int decr = 1 + (hash % (keyLength - 2));
            for (;;) {
                keyIdx -= decr;
                if (keyIdx < 0) {
                    keyIdx += keyLength;
                }
                if (states[keyIdx] == FREE) {
                    return null;
                }
                if (states[keyIdx] == FULL && keys[keyIdx] == key) {
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

    public int capacity() {
        return _keys.length;
    }

    public void clear() {
        Arrays.fill(_states, FREE);
        this._used = 0;
    }

    protected void ensureCapacity(int newCapacity) {
        int prime = Primes.findLeastPrimeNumber(newCapacity);
        rehash(prime);
        this._threshold = Math.round(prime * _loadFactor);
    }

    @SuppressWarnings("unchecked")
    private void rehash(int newCapacity) {
        int oldCapacity = _keys.length;
        if (newCapacity <= oldCapacity) {
            throw new IllegalArgumentException("new: " + newCapacity + ", old: " + oldCapacity);
        }
        final int[] newkeys = new int[newCapacity];
        final V[] newValues = (V[]) new Object[newCapacity];
        final byte[] newStates = new byte[newCapacity];
        int used = 0;
        for (int i = 0; i < oldCapacity; i++) {
            if (_states[i] == FULL) {
                used++;
                int k = _keys[i];
                V v = _values[i];
                int hash = keyHash(k);
                int keyIdx = hash % newCapacity;
                if (newStates[keyIdx] == FULL) {// second hashing
                    int decr = 1 + (hash % (newCapacity - 2));
                    while (newStates[keyIdx] != FREE) {
                        keyIdx -= decr;
                        if (keyIdx < 0) {
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

    private static int keyHash(final int key) {
        return key & 0x7fffffff;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeFloat(_loadFactor);
        out.writeFloat(_growFactor);
        out.writeInt(_used);

        final int size = _keys.length;
        out.writeInt(size);

        for (int i = 0; i < size; i++) {
            out.writeInt(_keys[i]);
            out.writeObject(_values[i]);
            out.writeByte(_states[i]);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this._loadFactor = in.readFloat();
        this._growFactor = in.readFloat();
        this._used = in.readInt();

        final int size = in.readInt();
        final int[] keys = new int[size];
        final Object[] values = new Object[size];
        final byte[] states = new byte[size];
        for (int i = 0; i < size; i++) {
            keys[i] = in.readInt();
            values[i] = in.readObject();
            states[i] = in.readByte();
        }
        this._threshold = size;
        this._keys = keys;
        this._values = (V[]) values;
        this._states = states;
    }

}
