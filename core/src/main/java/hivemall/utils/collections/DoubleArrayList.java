/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package hivemall.utils.collections;

import java.io.Serializable;

public final class DoubleArrayList implements Serializable {
    private static final long serialVersionUID = -8155789759545975413L;

    public static final int DEFAULT_CAPACITY = 12;

    /** array entity */
    private double[] data;
    private int used;

    public DoubleArrayList() {
        this(DEFAULT_CAPACITY);
    }

    public DoubleArrayList(int size) {
        this.data = new double[size];
        this.used = 0;
    }

    public DoubleArrayList(double[] initValues) {
        this.data = initValues;
        this.used = initValues.length;
    }

    public void add(double value) {
        if (used >= data.length) {
            expand(used + 1);
        }
        data[used++] = value;
    }

    public void add(double[] values) {
        final int needs = used + values.length;
        if (needs >= data.length) {
            expand(needs);
        }
        System.arraycopy(values, 0, data, used, values.length);
        this.used = needs;
    }

    /**
     * dynamic expansion.
     */
    private void expand(int max) {
        while (data.length < max) {
            final int len = data.length;
            double[] newArray = new double[len * 2];
            System.arraycopy(data, 0, newArray, 0, len);
            this.data = newArray;
        }
    }

    public double remove() {
        return data[--used];
    }

    public double remove(int index) {
        final double ret;
        if (index > used) {
            throw new IndexOutOfBoundsException();
        } else if (index == used) {
            ret = data[--used];
        } else { // index < used
            // removed value
            ret = data[index];
            final double[] newarray = new double[--used];
            // prefix
            System.arraycopy(data, 0, newarray, 0, index - 1);
            // appendix
            System.arraycopy(data, index + 1, newarray, index, used - index);
            // set fields.
            this.data = newarray;
        }
        return ret;
    }

    public void set(int index, double value) {
        if (index > used) {
            throw new IllegalArgumentException("Index MUST be less than \"size()\".");
        } else if (index == used) {
            ++used;
        }
        data[index] = value;
    }

    public double get(int index) {
        if (index >= used)
            throw new IndexOutOfBoundsException();
        return data[index];
    }

    public double fastGet(int index) {
        return data[index];
    }

    public int size() {
        return used;
    }

    public boolean isEmpty() {
        return used == 0;
    }

    public void clear() {
        used = 0;
    }

    public double[] toArray() {
        final double[] newArray = new double[used];
        System.arraycopy(data, 0, newArray, 0, used);
        return newArray;
    }

    public double[] array() {
        return data;
    }

    @Override
    public String toString() {
        final StringBuilder buf = new StringBuilder();
        buf.append('[');
        for (int i = 0; i < used; i++) {
            if (i != 0) {
                buf.append(", ");
            }
            buf.append(data[i]);
        }
        buf.append(']');
        return buf.toString();
    }
}
