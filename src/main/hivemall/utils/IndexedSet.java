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
package hivemall.utils;

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
        if(actualSize < expectedSize) {
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
        if(v == null) {
            int i = _list.size();
            _list.add(e);
            _map.put(e, i);
            return i;
        }
        return v.intValue();
    }

    @Override
    public boolean add(E e) {
        if(_map.containsKey(e)) {
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
        for(int i = 0; i < size; i++) {
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
        if(size > 0) {
            for(int i = 0; i < size; i++) {
                E v = _list.get(i);
                out.writeObject(v);
            }
        }
    }
}
