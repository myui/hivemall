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
package hivemall.common;

import hivemall.utils.lang.ArrayUtils;

import java.util.Random;

import org.apache.hadoop.hive.ql.metadata.HiveException;

public final class RandomizedAmplifier<T> {

    private final int numBuffers;
    private final int xtimes;

    private final AgedObject<T>[][] slots;
    private int position;

    private final Random[] randoms;

    private DropoutListener<T> listener = null;

    @SuppressWarnings("unchecked")
    public RandomizedAmplifier(int numBuffers, int xtimes) {
        if (numBuffers < 1) {
            throw new IllegalArgumentException("numBuffers must be greater than 0: " + numBuffers);
        }
        if (xtimes < 1) {
            throw new IllegalArgumentException("xtime must be greater than 0: " + xtimes);
        }
        this.numBuffers = numBuffers;
        this.xtimes = xtimes;
        this.slots = new AgedObject[xtimes][numBuffers];
        this.position = 0;
        this.randoms = new Random[xtimes];
        for (int i = 0; i < xtimes; i++) {
            randoms[i] = new Random();
        }
    }

    @SuppressWarnings("unchecked")
    public RandomizedAmplifier(int numBuffers, int xtimes, long seed) {
        if (numBuffers < 1) {
            throw new IllegalArgumentException("numBuffers must be greater than 0: " + numBuffers);
        }
        if (xtimes < 1) {
            throw new IllegalArgumentException("xtime must be greater than 0: " + xtimes);
        }
        this.numBuffers = numBuffers;
        this.xtimes = xtimes;
        this.slots = new AgedObject[xtimes][numBuffers];
        this.position = 0;
        this.randoms = new Random[xtimes];
        for (int i = 0; i < xtimes; i++) {
            randoms[i] = new Random(seed + i);
        }
    }

    public void setDropoutListener(DropoutListener<T> listener) {
        this.listener = listener;
    }

    public void add(T storedObj) throws HiveException {
        if (position < numBuffers) {
            for (int x = 0; x < xtimes; x++) {
                slots[x][position] = new AgedObject<T>(storedObj);
            }
            position++;
            if (position == numBuffers) {
                for (int x = 0; x < xtimes; x++) {
                    ArrayUtils.shuffle(slots[x], randoms[x]);
                }
            }
        } else {
            for (int x = 0; x < xtimes; x++) {
                AgedObject<T>[] slot = slots[x];
                Random rnd = randoms[x];
                int rindex1 = rnd.nextInt(numBuffers);
                int rindex2 = rnd.nextInt(numBuffers);
                AgedObject<T> replaced1 = slot[rindex1];
                AgedObject<T> replaced2 = slot[rindex2];
                assert (replaced1 != null);
                assert (replaced2 != null);
                if (replaced1.timestamp >= replaced2.timestamp) {// bias to hold old entry
                    dropout(replaced1.object);
                    replaced1.set(storedObj);
                } else {
                    dropout(replaced2.object);
                    replaced2.set(storedObj);
                }
            }
        }
    }

    public void sweepAll() throws HiveException {
        if (position < numBuffers && position > 1) {// shuffle an unfilled buffer
            for (int x = 0; x < xtimes; x++) {
                ArrayUtils.shuffle(slots[x], position, randoms[x]);
            }
        }
        for (int i = 0; i < numBuffers; i++) {
            for (int x = 0; x < xtimes; x++) {
                AgedObject<T>[] slot = slots[x];
                AgedObject<T> sweepedObj = slot[i];
                if (sweepedObj != null) {
                    dropout(sweepedObj.object);
                    slot[i] = null;
                }
            }
        }
    }

    protected void dropout(T droppped) throws HiveException {
        if (droppped == null) {
            throw new IllegalStateException("Illegal condition that dropped object is null");
        }
        if (listener != null) {
            listener.onDrop(droppped);
        }
    }

    private static final class AgedObject<T> {

        private T object;
        private long timestamp;

        AgedObject(T obj) {
            this.object = obj;
            this.timestamp = System.nanoTime();
        }

        void set(T object) {
            this.object = object;
            this.timestamp = System.nanoTime();
        }
    }

    public interface DropoutListener<T> {
        void onDrop(T droppped) throws HiveException;
    }

}
