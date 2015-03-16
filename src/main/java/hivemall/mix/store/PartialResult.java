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

import javax.annotation.Nonnegative;
import javax.annotation.concurrent.GuardedBy;

public abstract class PartialResult {

    private final Lock lock;

    @GuardedBy("lock()")
    protected short totalClock;

    public PartialResult() {
        this.totalClock = 0;
        this.lock = new TTASLock();
    }

    public final void lock() {
        lock.lock();
    }

    public final void unlock() {
        lock.unlock();
    }

    public abstract void add(float localWeight, float covar, short clock, @Nonnegative int deltaUpdates, float scale);

    public abstract void subtract(float localWeight, float covar, @Nonnegative int deltaUpdates, float scale);

    public abstract float getWeight(float scale);

    public abstract float getCovariance(float scale);

    public final short getClock() {
        return totalClock;
    }

    protected final void incrClock(short clock) {
        totalClock += clock;
    }

    public final int diffClock(short clock) {
        short diff = (short) (totalClock - clock);
        return diff < 0 ? -diff : diff;
    }

}
