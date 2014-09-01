/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013-2014
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
package hivemall.mix.server;

import hivemall.utils.lock.Lock;
import hivemall.utils.lock.TTASLock;

public abstract class PartialResult {

    private final Lock lock;

    protected float minCovariance;
    protected short totalClock;

    public PartialResult() {
        this.lock = new TTASLock();
    }

    public void lock() {
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }

    public abstract void add(float localWeight, float covar, short clock, int deltaUpdates);

    public abstract float getWeight();

    public float getMinCovariance() {
        return minCovariance;
    }

    protected void setMinCovariance(float covar) {
        this.minCovariance = Math.max(minCovariance, covar);
    }

    public short getClock() {
        return totalClock;
    }

    protected void incrClock(short clock) {
        totalClock += clock;
    }

    public int diffClock(short clock) {
        short diff = (short) (totalClock - clock);
        return diff < 0 ? -diff : diff;
    }

}
