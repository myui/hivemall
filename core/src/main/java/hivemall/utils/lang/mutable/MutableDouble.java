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

import javax.annotation.Nonnull;

public final class MutableDouble extends Number implements Copyable<MutableDouble>,
        Comparable<MutableDouble>, Serializable {
    private static final long serialVersionUID = 3275291486084936953L;

    private double value;

    public MutableDouble() {
        super();
    }

    public MutableDouble(double value) {
        super();
        this.value = value;
    }

    public MutableDouble(Number value) {
        super();
        this.value = value.doubleValue();
    }

    public void addValue(double o) {
        value += o;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public void setValue(Number value) {
        this.value = value.doubleValue();
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
        return (float) value;
    }

    @Override
    public double doubleValue() {
        return value;
    }

    @Override
    public void copyTo(MutableDouble another) {
        another.setValue(value);
    }

    @Override
    public void copyFrom(MutableDouble another) {
        this.value = another.value;
    }

    @Override
    public int compareTo(MutableDouble other) {
        return Double.compare(value, other.value);
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof MutableDouble)
                && (Double.doubleToLongBits(((MutableDouble) obj).value) == Double.doubleToLongBits(value));
    }

    @Override
    public int hashCode() {
        long bits = Double.doubleToLongBits(value);
        return (int) (bits ^ (bits >>> 32));
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }

    @Nonnull
    public static MutableDouble[] initArray(int size, double defaultValue) {
        final MutableDouble[] array = new MutableDouble[size];
        for (int i = 0; i < size; i++) {
            array[i] = new MutableDouble(defaultValue);
        }
        return array;
    }

}
