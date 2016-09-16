/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2016 Makoto YUI
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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Comparable list is a class which can be sorted or calculated unique lists.
 * @param <T>
 */
public class ComparableList<T extends Comparable<? super T>>
        extends LinkedList<T>
        implements Comparable<ComparableList<T>> {

    public ComparableList() {
        super();
    }

    public ComparableList(ComparableList<T> l) {
        super(l);
    }

    public ComparableList(List<T> l){
        super(l);
    }

    public int compareTo(ComparableList<T> comparableList) {
        Iterator<T> iterFirst  = iterator();
        Iterator<T> iterSecond = comparableList.iterator();
        while (iterFirst.hasNext() && iterSecond.hasNext()) {
            T   first  = iterFirst .next();
            T   second = iterSecond.next();
            int cmp    = first.compareTo(second);
            if (cmp == 0) {
                continue;
            }
            return cmp;
        }
        if (iterFirst.hasNext()) {
            return 1;
        }
        if (iterSecond.hasNext()) {
            return -1;
        }
        return 0;
    }
}