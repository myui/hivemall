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
package hivemall.utils.lang;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public final class Identifier<E> implements Serializable {
    private static final long serialVersionUID = 7949630590734361716L;

    private final Map<E, Integer> counts;
    private int sequence;

    public Identifier() {
        this.counts = new HashMap<E, Integer>(512);
        this.sequence = -1;
    }

    public int valueOf(E key) {
        Integer count = counts.get(key);
        if(count == null) {
            ++sequence;
            counts.put(key, Integer.valueOf(sequence));
            return sequence;
        } else {
            return count.intValue();
        }
    }

}
