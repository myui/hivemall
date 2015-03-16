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
package hivemall.utils.lang.mutable;

import hivemall.utils.lang.Copyable;

import java.io.Serializable;

public final class MutableInt extends Number
        implements Copyable<MutableInt>, Comparable<MutableInt>, Serializable {
    private static final long serialVersionUID = -3289272606407100628L;

    private int value;

    public MutableInt() {
        super();
    }

    public MutableInt(int value) {
        super();
        this.value = value;
    }

    public MutableInt(Number value) {
        super();
        this.value = value.intValue();
    }

    public void addValue(int o) {
        value += o;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public void setValue(Number value) {
        this.value = value.intValue();
    }

    @Override
    public int intValue() {
        return value;
    }

    @Override
    public long longValue() {
        return value;
    }

    @Override
    public float floatValue() {
        return value;
    }

    @Override
    public double doubleValue() {
        return value;
    }

    @Override
    public void copyTo(MutableInt another) {
        another.setValue(value);
    }

    @Override
    public void copyFrom(MutableInt another) {
        this.value = another.value;
    }

    @Override
    public int compareTo(MutableInt other) {
        return compare(value, other.value);
    }

    private static int compare(final int x, final int y) {
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof MutableInt) {
            return value == ((MutableInt) obj).intValue();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return value;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }

}
