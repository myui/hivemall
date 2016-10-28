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

import javax.annotation.Nonnull;

public final class FixedIntArray implements IntArray {
    private static final long serialVersionUID = -1450212841013810240L;

    @Nonnull
    private final int[] array;
    private final int size;

    public FixedIntArray(@Nonnull int size) {
        this.array = new int[size];
        this.size = size;
    }

    public FixedIntArray(@Nonnull int[] array) {
        this.array = array;
        this.size = array.length;
    }

    @Override
    public int get(int index) {
        return array[index];
    }

    @Override
    public int get(int index, int valueIfKeyNotFound) {
        if (index >= size) {
            return valueIfKeyNotFound;
        }
        return array[index];
    }

    @Override
    public void put(int index, int value) {
        array[index] = value;
    }

    @Override
    public int size() {
        return array.length;
    }

    @Override
    public int keyAt(int index) {
        return index;
    }

}
