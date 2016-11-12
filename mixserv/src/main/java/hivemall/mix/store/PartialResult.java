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
package hivemall.mix.store;

import hivemall.utils.lock.Lock;
import hivemall.utils.lock.TTASLock;

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

    public abstract void add(float localWeight, float covar, @Nonnegative int deltaUpdates,
            float scale);

    public abstract void subtract(float localWeight, float covar, @Nonnegative int deltaUpdates,
            float scale);

    public abstract float getWeight(float scale);

    public abstract float getCovariance(float scale);

    public final short getClock() {
        return globalClock;
    }

    protected final void incrClock(int deltaUpdates) {
        globalClock += deltaUpdates;
    }

    // Return diff between global and local clocks.
    // This implementation depends on the overflow/underflow behavior of short-typed values.
    // i.e., [-32768...l...g...32768) is one of clock examples.
    // Label 'l' and 'g' represent local and global clocks, respectively.
    // In this case, it returns a minimum value, l...g or g...l.
    public final int diffClock(final short localClock) {
        short tempValue1 = globalClock;
        tempValue1 -= localClock;
        short tempValue2 = localClock;
        tempValue2 -= globalClock;
        return Math.min(Math.abs(tempValue1), Math.abs(tempValue2));
    }

}
