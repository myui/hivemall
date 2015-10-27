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
package hivemall.mix.store;

import hivemall.utils.lock.Lock;
import hivemall.utils.lock.TTASLock;
import hivemall.utils.math.MathUtils;

import javax.annotation.Nonnegative;
import javax.annotation.concurrent.GuardedBy;

public abstract class PartialResult {

    private final Lock lock;

    @GuardedBy("lock()")
    protected short globalClock;

    public PartialResult() {
        this.globalClock = 0;
        this.lock = new TTASLock();
    }

    public final void lock() {
        lock.lock();
    }

    public final void unlock() {
        lock.unlock();
    }

    public abstract void add(float localWeight, float covar, @Nonnegative int deltaUpdates, float scale);

    public abstract void subtract(float localWeight, float covar, @Nonnegative int deltaUpdates, float scale);

    public abstract float getWeight(float scale);

    public abstract float getCovariance(float scale);

    public final short getClock() {
        return globalClock;
    }

    protected final void incrClock(int deltaUpdates) {
        globalClock += deltaUpdates;
    }

    public final short diffClock(short localClock) {
        int dist = globalClock - localClock;
        if(dist < 0) {
            dist = -dist;
        }
        final short ret;
        if(MathUtils.sign(globalClock) == MathUtils.sign(localClock)) {
            ret = (short) dist;
        } else {
            int diff;
            if(globalClock < 0) {
                diff = globalClock - Short.MIN_VALUE;
                assert (diff >= 0) : "diff: " + diff + ", globalClock: " + globalClock;
            } else {
                diff = Short.MAX_VALUE - globalClock;
            }
            if(localClock < 0) {
                int tmp = localClock - Short.MIN_VALUE;
                assert (tmp >= 0) : "diff localCkicj: " + tmp + ", localClock: " + localClock;
                diff += tmp;
            } else {
                diff += Short.MAX_VALUE - localClock;
            }
            assert (diff >= 0) : diff;
            if(dist < diff) {
                ret = (short) dist;
            } else {
                ret = (short) diff;
            }
        }
        return ret;
    }

}
