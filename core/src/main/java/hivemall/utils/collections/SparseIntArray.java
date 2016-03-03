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
package hivemall.utils.collections;

import hivemall.utils.lang.ArrayUtils;

import java.util.Arrays;

public final class SparseIntArray implements IntArray {
    private static final long serialVersionUID = -2814248784231540118L;

    private int[] mKeys;
    private int[] mValues;
    private int mSize;

    public SparseIntArray() {
        this(10);
    }

    public SparseIntArray(int initialCapacity) {
        mKeys = new int[initialCapacity];
        mValues = new int[initialCapacity];
        mSize = 0;
    }

    private SparseIntArray(int[] mKeys, int[] mValues, int mSize) {
        this.mKeys = mKeys;
        this.mValues = mValues;
        this.mSize = mSize;
    }

    public IntArray deepCopy() {
        int[] newKeys = new int[mSize];
        int[] newValues = new int[mSize];
        System.arraycopy(mKeys, 0, newKeys, 0, mSize);
        System.arraycopy(mValues, 0, newValues, 0, mSize);
        return new SparseIntArray(newKeys, newValues, mSize);
    }

    @Override
    public int get(int key) {
        return get(key, 0);
    }

    @Override
    public int get(int key, int valueIfKeyNotFound) {
        int i = Arrays.binarySearch(mKeys, 0, mSize, key);
        if (i < 0) {
            return valueIfKeyNotFound;
        } else {
            return mValues[i];
        }
    }

    public void delete(int key) {
        int i = Arrays.binarySearch(mKeys, 0, mSize, key);
        if (i >= 0) {
            removeAt(i);
        }
    }

    public void removeAt(int index) {
        System.arraycopy(mKeys, index + 1, mKeys, index, mSize - (index + 1));
        System.arraycopy(mValues, index + 1, mValues, index, mSize - (index + 1));
        mSize--;
    }

    @Override
    public void put(int key, int value) {
        int i = Arrays.binarySearch(mKeys, 0, mSize, key);
        if (i >= 0) {
            mValues[i] = value;
        } else {
            i = ~i;
            mKeys = ArrayUtils.insert(mKeys, mSize, i, key);
            mValues = ArrayUtils.insert(mValues, mSize, i, value);
            mSize++;
        }
    }

    public void increment(int key, int value) {
        int i = Arrays.binarySearch(mKeys, 0, mSize, key);
        if (i >= 0) {
            mValues[i] += value;
        } else {
            i = ~i;
            mKeys = ArrayUtils.insert(mKeys, mSize, i, key);
            mValues = ArrayUtils.insert(mValues, mSize, i, value);
            mSize++;
        }
    }

    @Override
    public int size() {
        return mSize;
    }

    @Override
    public int keyAt(int index) {
        return mKeys[index];
    }

    public int valueAt(int index) {
        return mValues[index];
    }

    public void setValueAt(int index, int value) {
        mValues[index] = value;
    }

    public int indexOfKey(int key) {
        return Arrays.binarySearch(mKeys, 0, mSize, key);
    }

    public int indexOfValue(int value) {
        for (int i = 0; i < mSize; i++) {
            if (mValues[i] == value) {
                return i;
            }
        }
        return -1;
    }

    public void clear() {
        mSize = 0;
    }

    public void append(int key, int value) {
        if (mSize != 0 && key <= mKeys[mSize - 1]) {
            put(key, value);
            return;
        }
        mKeys = ArrayUtils.append(mKeys, mSize, key);
        mValues = ArrayUtils.append(mValues, mSize, value);
        mSize++;
    }

    @Override
    public String toString() {
        if (size() <= 0) {
            return "{}";
        }

        StringBuilder buffer = new StringBuilder(mSize * 28);
        buffer.append('{');
        for (int i = 0; i < mSize; i++) {
            if (i > 0) {
                buffer.append(", ");
            }
            int key = keyAt(i);
            buffer.append(key);
            buffer.append('=');
            int value = valueAt(i);
            buffer.append(value);
        }
        buffer.append('}');
        return buffer.toString();
    }
}
