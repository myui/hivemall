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
package hivemall.utils.lang;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class Counter<E> implements Serializable {
    private static final long serialVersionUID = 7949630590734361716L;

    private final Map<E, Integer> counts;

    public Counter() {
        this.counts = new HashMap<E, Integer>();
    }

    public Counter(@Nonnull Map<E, Integer> counts) {
        this.counts = counts;
    }

    public Map<E, Integer> getMap() {
        return counts;
    }

    public int increment(E key) {
        return increment(key, 1);
    }

    public int increment(E key, int amount) {
        Integer count = counts.get(key);
        if (count == null) {
            counts.put(key, Integer.valueOf(amount));
            return 0;
        } else {
            int old = count.intValue();
            counts.put(key, Integer.valueOf(old + amount));
            return old;
        }
    }

    public int getCount(E key) {
        Integer count = counts.get(key);
        if (count == null) {
            return 0;
        } else {
            return count.intValue();
        }
    }

    public void addAll(Map<E, Integer> counter) {
        if (counter == null) {
            return;
        }
        for (Map.Entry<E, Integer> e : counter.entrySet()) {
            increment(e.getKey(), e.getValue().intValue());
        }
    }

    public void addAll(Counter<E> counter) {
        if (counter == null) {
            return;
        }
        for (Map.Entry<E, Integer> e : counter.entrySet()) {
            increment(e.getKey(), e.getValue().intValue());
        }
    }

    public Set<Map.Entry<E, Integer>> entrySet() {
        return counts.entrySet();
    }

    @Nullable
    public E whichMax() {
        E maxKey = null;
        int maxValue = Integer.MIN_VALUE;
        for (Map.Entry<E, Integer> e : counts.entrySet()) {
            int v = e.getValue().intValue();
            if (v >= maxValue) {
                maxValue = v;
                maxKey = e.getKey();
            }
        }
        return maxKey;
    }

    @Nullable
    public E whichMin() {
        E minKey = null;
        int minValue = Integer.MAX_VALUE;
        for (Map.Entry<E, Integer> e : counts.entrySet()) {
            int v = e.getValue().intValue();
            if (v <= minValue) {
                minValue = v;
                minKey = e.getKey();
            }
        }
        return minKey;
    }

    public int size() {
        return counts.size();
    }

}
