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

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public final class Identifier<T> implements Serializable {
    private static final long serialVersionUID = 7949630590734361716L;

    private final Map<T, Integer> counts;
    private int sequence;

    public Identifier() {
        this(0);
    }

    public Identifier(int initSeq) {
        this.counts = new HashMap<>(512);
        this.sequence = initSeq;
    }

    public int valueOf(@Nonnull T key) {
        Integer count = counts.get(key);
        if (count == null) {
            int id = sequence;
            counts.put(key, Integer.valueOf(id));
            ++sequence;
            return id;
        } else {
            return count.intValue();
        }
    }

    public void put(@Nonnull T key) {
        Integer count = counts.get(key);
        if (count != null) {
            return;
        }
        int id = sequence;
        counts.put(key, Integer.valueOf(id));
        ++sequence;
    }

    public Map<T, Integer> getMap() {
        return counts;
    }

    public int size() {
        return sequence;
    }

}
