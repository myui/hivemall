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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

/**
 * An open-addressing hash table with double hashing
 * 
 * @see http://en.wikipedia.org/wiki/Double_hashing
 */
public class IntOpenHashMap<V> implements Externalizable {
    private static final long serialVersionUID = -8162355845665353513L;

    protected static final byte FREE = 0;
    protected static final byte FULL = 1;
    protected static final byte REMOVED = 2;

    private static final float DEFAULT_LOAD_FACTOR = 0.7f;
    private static final float DEFAULT_GROW_FACTOR = 2.0f;

    protected final transient float _loadFactor;
    protected final transient float _growFactor;

    protected int _used = 0;
    protected int _threshold;

    protected int[] _keys;
    protected V[] _values;
    protected byte[] _states;

    @SuppressWarnings("unchecked")
    protected IntOpenHashMap(int size, float loadFactor, float growFactor, boolean forcePrime) {
        if(size < 1) {
            throw new IllegalArgumentException();
        }
        this._loadFactor = loadFactor;
        this._growFactor = growFactor;
        int actualSize = forcePrime ? Primes.findLeastPrimeNumber(size) : size;
        this._keys = new int[actualSize];
        this._values = (V[]) new Object[actualSize];
        this._states = new byte[actualSize];
        this._threshold = Math.round(actualSize * _loadFactor);
    }

    public IntOpenHashMap(int size, float loadFactor, float growFactor) {
        this(size, loadFactor, growFactor, true);
    }

    public IntOpenHashMap(int size) {
        this(size, DEFAULT_LOAD_FACTOR, DEFAULT_GROW_FACTOR, true);
    }

    public IntOpenHashMap() {// required for serialization
        this._loadFactor = DEFAULT_LOAD_FACTOR;
        this._growFactor = DEFAULT_GROW_FACTOR;
    }

    public boolean containsKey(int key) {
        return findKey(key) >= 0;
    }

    public final V get(final int key) {
        final int i = findKey(key);
        if(i < 0) {
            return null;
        }
        recordAccess(i);
        return _values[i];
    }

    public V put(final int key, final V value) {
        final int hash = keyHash(key);
        int keyLength = _keys.length;
        int keyIdx = hash % keyLength;

        final boolean expanded = preAddEntry(keyIdx);
        if(expanded) {
            keyLength = _keys.length;
            keyIdx = hash % keyLength;
        }

        final int[] keys = _keys;
        final V[] values = _values;
        final byte[] states = _states;

        if(states[keyIdx] == FULL) {// double hashing
            if(keys[keyIdx] == key) {
                V old = values[keyIdx];
                values[keyIdx] = value;
                recordAccess(keyIdx);
                return old;
            }
            // try second hash
            final int decr = 1 + (hash % (keyLength - 2));
            for(;;) {
                keyIdx -= decr;
                if(keyIdx < 0) {
                    keyIdx += keyLength;
                }
                if(isFree(keyIdx, key)) {
                    break;
                }
                if(states[keyIdx] == FULL && keys[keyIdx] == key) {
                    V old = values[keyIdx];
                    values[keyIdx] = value;
                    recordAccess(keyIdx);
                    return old;
                }
            }
        }
        keys[keyIdx] = key;
        values[keyIdx] = value;
        states[keyIdx] = FULL;
        ++_used;
        postAddEntry(keyIdx);
        return null;
    }

    public V putIfAbsent(final int key, final V value) {
        final int hash = keyHash(key);
        int keyLength = _keys.length;
        int keyIdx = hash % keyLength;

        final boolean expanded = preAddEntry(keyIdx);
        if(expanded) {
            keyLength = _keys.length;
            keyIdx = hash % keyLength;
        }

        final int[] keys = _keys;
        final V[] values = _values;
        final byte[] states = _states;

        if(states[keyIdx] == FULL) {// second hashing
            if(keys[keyIdx] == key) {
                return values[keyIdx];
            }
            // try second hash
            final int decr = 1 + (hash % (keyLength - 2));
            for(;;) {
                keyIdx -= decr;
                if(keyIdx < 0) {
                    keyIdx += keyLength;
                }
                if(isFree(keyIdx, key)) {
                    break;
                }
                if(states[keyIdx] == FULL && keys[keyIdx] == key) {
                    return values[keyIdx];
                }
            }
        }
        keys[keyIdx] = key;
        values[keyIdx] = value;
        states[keyIdx] = FULL;
        _used++;
        postAddEntry(keyIdx);
        return null;
    }

    /** Return weather the required slot is free for new entry */
    protected boolean isFree(int index, int key) {
        byte stat = _states[index];
        if(stat == FREE) {
            return true;
        }
        if(stat == REMOVED && _keys[index] == key) {
            return true;
        }
        return false;
    }

    /** @return expanded or not */
    protected boolean preAddEntry(int index) {
        if((_used + 1) >= _threshold) {// too filled
            int newCapacity = Math.round(_keys.length * _growFactor);
            ensureCapacity(newCapacity);
            return true;
        }
        return false;
    }

    protected void postAddEntry(int index) {}

    private int findKey(int key) {
        int[] keys = _keys;
        byte[] states = _states;
        int keyLength = keys.length;

        int hash = keyHash(key);
        int keyIdx = hash % keyLength;
        if(states[keyIdx] != FREE) {
            if(states[keyIdx] == FULL && keys[keyIdx] == key) {
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
                if(states[keyIdx] == FULL && keys[keyIdx] == key) {
                    return keyIdx;
                }
            }
        }
        return -1;
    }

    public V remove(int key) {
        int[] keys = _keys;
        V[] values = _values;
        byte[] states = _states;
        int keyLength = keys.length;

        int hash = keyHash(key);
        int keyIdx = hash % keyLength;
        if(states[keyIdx] != FREE) {
            if(states[keyIdx] == FULL && keys[keyIdx] == key) {
                V old = values[keyIdx];
                states[keyIdx] = REMOVED;
                --_used;
                recordRemoval(keyIdx);
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
                if(states[keyIdx] == FULL && keys[keyIdx] == key) {
                    V old = values[keyIdx];
                    states[keyIdx] = REMOVED;
                    --_used;
                    recordRemoval(keyIdx);
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

    @SuppressWarnings("unchecked")
    public IMapIterator<V> entries() {
        return new MapIterator();
    }

    @Override
    public String toString() {
        int len = size() * 10 + 2;
        StringBuilder buf = new StringBuilder(len);
        buf.append('{');
        IMapIterator<V> i = entries();
        while(i.next() != -1) {
            buf.append(i.getKey());
            buf.append('=');
            buf.append(i.getValue());
            if(i.hasNext()) {
                buf.append(',');
            }
        }
        buf.append('}');
        return buf.toString();
    }

    private void ensureCapacity(int newCapacity) {
        int prime = Primes.findLeastPrimeNumber(newCapacity);
        rehash(prime);
        this._threshold = Math.round(prime * _loadFactor);
    }

    @SuppressWarnings("unchecked")
    protected void rehash(int newCapacity) {
        int oldCapacity = _keys.length;
        if(newCapacity <= oldCapacity) {
            throw new IllegalArgumentException("new: " + newCapacity + ", old: " + oldCapacity);
        }
        final int[] oldKeys = _keys;
        final V[] oldValues = _values;
        final byte[] oldStates = _states;
        int[] newkeys = new int[newCapacity];
        V[] newValues = (V[]) new Object[newCapacity];
        byte[] newStates = new byte[newCapacity];
        int used = 0;
        for(int i = 0; i < oldCapacity; i++) {
            if(oldStates[i] == FULL) {
                used++;
                int k = oldKeys[i];
                V v = oldValues[i];
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
                newkeys[keyIdx] = k;
                newValues[keyIdx] = v;
                newStates[keyIdx] = FULL;
            }
        }
        this._keys = newkeys;
        this._values = newValues;
        this._states = newStates;
        this._used = used;
    }

    private static int keyHash(int key) {
        return key & 0x7fffffff;
    }

    protected void recordAccess(int idx) {};

    protected void recordRemoval(int idx) {};

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(_threshold);
        out.writeInt(_used);

        out.writeInt(_keys.length);
        IMapIterator<V> i = entries();
        while(i.next() != -1) {
            out.writeInt(i.getKey());
            out.writeObject(i.getValue());
        }
    }

    @SuppressWarnings("unchecked")
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this._threshold = in.readInt();
        this._used = in.readInt();

        int keylen = in.readInt();
        int[] keys = new int[keylen];
        V[] values = (V[]) new Object[keylen];
        byte[] states = new byte[keylen];
        for(int i = 0; i < _used; i++) {
            int k = in.readInt();
            V v = (V) in.readObject();
            int hash = keyHash(k);
            int keyIdx = hash % keylen;
            if(states[keyIdx] != FREE) {// second hash
                int decr = 1 + (hash % (keylen - 2));
                for(;;) {
                    keyIdx -= decr;
                    if(i < 0) {
                        keyIdx += keylen;
                    }
                    if(states[keyIdx] == FREE) {
                        break;
                    }
                }
            }
            states[keyIdx] = FULL;
            keys[keyIdx] = k;
            values[keyIdx] = v;
        }
        this._keys = keys;
        this._values = values;
        this._states = states;
    }

    public interface IMapIterator<V> {

        public boolean hasNext();

        public int next();

        public int getKey();

        public V getValue();

    }

    @SuppressWarnings("rawtypes")
    private final class MapIterator implements IMapIterator {

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
            this.lastEntry = curEntry;
            this.nextEntry = nextEntry(curEntry + 1);
            return curEntry;
        }

        public int getKey() {
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

    public static final class IntOpenLRUMap<V> extends IntOpenHashMap<V> {
        private static final long serialVersionUID = -1884240205310136433L;

        private final int maxCapacity;
        private final ChainEntry evictChainHeader;
        private boolean reachedLimit = false;
        private ChainEntry[] _entries;

        /**
         * allocates twice more open rooms for hash than limit, and 
         * the hash table is fixed.
         */
        public IntOpenLRUMap(int limit) {
            super(limit * 2, 1.0f, limit, true);
            this.maxCapacity = limit;
            this._entries = new ChainEntry[_keys.length];
            this.evictChainHeader = initEntryChain();
        }

        /**
         * growFactor is limit / init.
         * This semantics means expansion occourrs at most once
         * when more than 75% of rooms are filled.
         */
        public IntOpenLRUMap(int init, int limit) {
            super(init, 0.75f, limit / init, true);
            if(limit < init) {
                throw new IllegalArgumentException("init '" + init + "'must be less than limit '"
                        + limit + '\'');
            }
            this.maxCapacity = limit;
            this._entries = new ChainEntry[_keys.length];
            this.evictChainHeader = initEntryChain();
        }

        private ChainEntry initEntryChain() {
            ChainEntry header = new ChainEntry(-1);
            header.prev = header.next = header;
            return header;
        }

        @Override
        protected boolean preAddEntry(int index) {
            ++_used;
            if(removeEldestEntry()) {
                this.reachedLimit = true;
                ChainEntry eldest = evictChainHeader.next;
                if(eldest == null) {
                    throw new IllegalStateException();
                }
                V removed = directRemove(eldest.entryIndex);
                if(removed == null) {
                    throw new IllegalStateException();
                }
            } else {
                // assert (_threshold == maxCapacity) : "threshold: " + _threshold + ", maxCapacity: " + maxCapacity;
                if(_used > _threshold) {// too filled
                    throw new IllegalStateException("expansion is required for elements limited map");
                }
            }
            return false;
        }

        private V directRemove(int index) {
            V old = _values[index];
            _states[index] = REMOVED;
            --_used;
            recordRemoval(index);
            return old;
        }

        @Override
        protected void postAddEntry(int idx) {
            ChainEntry newEntry = new ChainEntry(idx);
            if(_entries[idx] != null) {
                throw new IllegalStateException();
            }
            _entries[idx] = newEntry;
            newEntry.addBefore(evictChainHeader);
        }

        /**
         * Move the entry to chain head.
         */
        @Override
        protected void recordAccess(int idx) {
            ChainEntry entry = _entries[idx];
            if(entry == null) {
                throw new IllegalStateException();
            }
            entry.remove();
            entry.addBefore(evictChainHeader);
        }

        /**
         * Remove the entry from the chain.
         */
        @Override
        protected void recordRemoval(int idx) {
            ChainEntry entry = _entries[idx];
            if(entry == null) {
                throw new IllegalStateException();
            }
            entry.remove();
            _entries[idx] = null;
        }

        private boolean removeEldestEntry() {
            return _used > maxCapacity;
        }

        @Override
        protected boolean isFree(int index, int key) {
            byte stat = _states[index];
            return (stat == FREE) || (stat == REMOVED);
        }

        /** Do not trust me, consider this as rough estimated size. */
        @Override
        public int size() {// TODO REVIEWME
            if(reachedLimit) {
                return maxCapacity;
            } else {
                return _used;
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public IMapIterator<V> entries() {
            return new OrderedMapIterator(evictChainHeader);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected final void rehash(int newCapacity) {
            int oldCapacity = _keys.length;
            if(newCapacity <= oldCapacity) {
                throw new IllegalArgumentException("new: " + newCapacity + ", old: " + oldCapacity);
            }
            final int[] oldKeys = _keys;
            final V[] oldValues = _values;
            final byte[] oldStates = _states;
            final ChainEntry[] oldEntries = _entries;
            final int[] newkeys = new int[newCapacity];
            final V[] newValues = (V[]) new Object[newCapacity];
            final byte[] newStates = new byte[newCapacity];
            ChainEntry[] newEntries = new ChainEntry[newCapacity];
            int used = 0;
            for(int i = 0; i < oldCapacity; i++) {
                if(oldStates[i] == FULL) {
                    used++;
                    int k = oldKeys[i];
                    V v = oldValues[i];
                    ChainEntry e = oldEntries[i];
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
                    newkeys[keyIdx] = k;
                    newValues[keyIdx] = v;
                    newStates[keyIdx] = FULL;
                    newEntries[keyIdx] = e;
                }
            }
            this._keys = newkeys;
            this._values = newValues;
            this._states = newStates;
            this._entries = newEntries;
            this._used = used;
        }

        static final class ChainEntry {

            final int entryIndex;
            ChainEntry prev = null, next = null;

            public ChainEntry(int index) {
                this.entryIndex = index;
            }

            /**
             * Removes this entry from the linked list.
             */
            private void remove() {
                prev.next = next;
                next.prev = prev;
                prev = null;
                next = null;
            }

            /**
             * Inserts this entry before the specified existing entry in the list.
             */
            private void addBefore(ChainEntry existingEntry) {
                next = existingEntry;
                prev = existingEntry.prev;
                prev.next = this;
                next.prev = this;
            }
        }

        @SuppressWarnings("rawtypes")
        final class OrderedMapIterator implements IMapIterator {

            private ChainEntry entry;

            OrderedMapIterator(ChainEntry e) {
                if(e == null) {
                    throw new IllegalArgumentException();
                }
                this.entry = e;
            }

            public int getKey() {
                return _keys[entry.entryIndex];
            }

            public V getValue() {
                return _values[entry.entryIndex];
            }

            public boolean hasNext() {
                return entry.next != evictChainHeader;
            }

            public int next() {
                ChainEntry e = entry.next;
                if(e == evictChainHeader) {
                    return -1;
                }
                this.entry = e;
                return e.entryIndex;
            }
        }
    }
}
