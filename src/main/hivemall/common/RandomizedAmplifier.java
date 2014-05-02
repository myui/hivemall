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
package hivemall.common;

import hivemall.utils.ArrayUtils;

import java.util.Random;

import org.apache.hadoop.hive.ql.metadata.HiveException;

public class RandomizedAmplifier<T> {

    private final int numBuffers;
    private final int xtimes;

    private final AgedObject<T>[][] slots;
    private int position;

    private final Random[] randoms;

    private DropoutListener<T> listener = null;

    @SuppressWarnings("unchecked")
    public RandomizedAmplifier(int numBuffers, int xtimes) {
        if(numBuffers < 1) {
            throw new IllegalArgumentException("numBuffers must be greater than 0: " + numBuffers);
        }
        if(xtimes < 1) {
            throw new IllegalArgumentException("xtime must be greater than 0: " + xtimes);
        }
        this.numBuffers = numBuffers;
        this.xtimes = xtimes;
        this.slots = new AgedObject[xtimes][numBuffers];
        this.position = 0;
        this.randoms = new Random[xtimes];
        for(int i = 0; i < xtimes; i++) {
            randoms[i] = new Random();
        }
    }

    public void setDropoutListener(DropoutListener<T> listener) {
        this.listener = listener;
    }

    public void add(T storedObj) throws HiveException {
        if(position < numBuffers) {
            for(int x = 0; x < xtimes; x++) {
                slots[x][position] = new AgedObject<T>(storedObj);
            }
            position++;
            if(position == numBuffers) {
                for(int x = 0; x < xtimes; x++) {
                    ArrayUtils.shuffle(slots[x], randoms[x]);
                }
            }
        } else {
            for(int x = 0; x < xtimes; x++) {
                AgedObject<T>[] slot = slots[x];
                Random rnd = randoms[x];
                int rindex1 = rnd.nextInt(numBuffers);
                int rindex2 = rnd.nextInt(numBuffers);
                AgedObject<T> replaced1 = slot[rindex1];
                AgedObject<T> replaced2 = slot[rindex2];
                assert (replaced1 != null);
                assert (replaced2 != null);
                if(replaced1.timestamp >= replaced2.timestamp) {// bias to hold old entry
                    dropout(replaced1.object);
                    slot[rindex1] = new AgedObject<T>(storedObj);
                } else {
                    dropout(replaced2.object);
                    slot[rindex2] = new AgedObject<T>(storedObj);
                }
            }
        }
    }

    public void sweepAll() throws HiveException {
        for(int i = 0; i < numBuffers; i++) {
            for(int x = 0; x < xtimes; x++) {
                AgedObject<T>[] slot = slots[x];
                AgedObject<T> sweepedObj = slot[i];
                if(sweepedObj != null) {
                    dropout(sweepedObj.object);
                    slot[i] = null;
                }
            }
        }
    }

    protected void dropout(T droppped) throws HiveException {
        if(droppped == null) {
            throw new IllegalStateException("Illegal condition that dropped object is null");
        }
        if(listener != null) {
            listener.onDrop(droppped);
        }
    }

    private static final class AgedObject<T> {

        private final T object;
        private final long timestamp;

        AgedObject(T obj) {
            this.object = obj;
            this.timestamp = System.nanoTime();
        }
    }

    public interface DropoutListener<T> {
        void onDrop(T droppped) throws HiveException;
    }

}
