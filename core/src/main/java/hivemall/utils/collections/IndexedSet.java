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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public final class IndexedSet<E> extends AbstractSet<E> implements Externalizable {
    private static final long serialVersionUID = 8775694634056054599L;

    private Map<E, Integer> _map;
    private ArrayList<E> _list;

    public IndexedSet() {
        this(256);
    }

    public IndexedSet(int size) {
        this._map = new HashMap<E, Integer>(size);
        this._list = new ArrayList<E>(size);
    }

    @SuppressWarnings("unchecked")
    public void ensureSize(int expectedSize) {
        int actualSize = _list.size();
        if (actualSize < expectedSize) {
            _list.ensureCapacity(expectedSize);
            int delta = expectedSize - actualSize;
            E[] ary = (E[]) new Object[delta];
            List<E> lst = Arrays.asList(ary);
            _list.addAll(lst);
        }
    }

    public int indexOf(E e) {
        Integer v = _map.get(e);
        return v == null ? -1 : v;
    }

    public int addIndexOf(E e) {
        final Integer v = _map.get(e);
        if (v == null) {
            int i = _list.size();
            _list.add(e);
            _map.put(e, i);
            return i;
        }
        return v.intValue();
    }

    @Override
    public boolean add(E e) {
        if (_map.containsKey(e)) {
            return true;
        } else {
            int i = _list.size();
            _list.add(e);
            _map.put(e, i);
            return false;
        }
    }

    public E get(int index) {
        return _list.get(index);
    }

    public void set(int index, E value) {
        _map.put(value, index);
        _list.set(index, value);
    }

    public Iterator<E> iterator() {
        return _map.keySet().iterator();
    }

    public int size() {
        return _map.size();
    }

    //--------------------------------------------------
    // Externalizable

    @SuppressWarnings("unchecked")
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        final int size = in.readInt();
        final Map<E, Integer> map = new HashMap<E, Integer>(size);
        final ArrayList<E> list = new ArrayList<E>(size);
        for (int i = 0; i < size; i++) {
            E e = (E) in.readObject();
            list.add(e);
            map.put(e, i);
        }
        this._map = map;
        this._list = list;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        final int size = _list.size();
        out.writeInt(size);
        if (size > 0) {
            for (int i = 0; i < size; i++) {
                E v = _list.get(i);
                out.writeObject(v);
            }
        }
    }
}
