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

public final class MutableFloat extends Number implements Copyable<MutableFloat>,
        Comparable<MutableFloat>, Serializable {
    private static final long serialVersionUID = 1758508142164954048L;

    private float value;

    public MutableFloat() {
        super();
    }

    public MutableFloat(float value) {
        super();
        this.value = value;
    }

    public MutableFloat(Number value) {
        super();
        this.value = value.floatValue();
    }

    public void addValue(float o) {
        value += o;
    }

    public float getValue() {
        return value;
    }

    public void setValue(float value) {
        this.value = value;
    }

    public void setValue(Number value) {
        this.value = value.floatValue();
    }

    @Override
    public int intValue() {
        return (int) value;
    }

    @Override
    public long longValue() {
        return (long) value;
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
    public void copyTo(MutableFloat another) {
        another.setValue(value);
    }

    @Override
    public void copyFrom(MutableFloat another) {
        this.value = another.value;
    }

    @Override
    public int compareTo(MutableFloat other) {
        return Float.compare(value, other.value);
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof MutableFloat)
                && (Float.floatToIntBits(((MutableFloat) obj).value) == Float.floatToIntBits(value));
    }

    @Override
    public int hashCode() {
        return Float.floatToIntBits(value);
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }

}
