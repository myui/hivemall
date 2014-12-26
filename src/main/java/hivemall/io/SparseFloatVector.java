/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013-2014
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
package hivemall.io;

import hivemall.utils.collections.Int2FloatOpenHash;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public final class SparseFloatVector {

    private final Int2FloatOpenHash entries;

    public SparseFloatVector() {
        this(136861);
    }

    public SparseFloatVector(int size) {
        this.entries = new Int2FloatOpenHash(size);
        entries.defaultReturnValue(0.f);
    }

    public float get(int i) {
        return entries.get(i);
    }

    public void set(int i, float value) {
        entries.put(i, value);
    }

}
