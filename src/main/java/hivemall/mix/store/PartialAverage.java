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
package hivemall.mix.store;

import javax.annotation.Nonnegative;
import javax.annotation.concurrent.GuardedBy;

public final class PartialAverage extends PartialResult {

    @GuardedBy("lock()")
    private double scaledSumWeights;
    @GuardedBy("lock()")
    private int totalUpdates;

    public PartialAverage() {
        super();
        this.scaledSumWeights = 0.d;
        this.totalUpdates = 0;
    }

    @Override
    public float getCovariance(float scale) {
        return 1.f;
    }

    @Override
    public void add(float localWeight, float covar, short clock, @Nonnegative int deltaUpdates, float scale) {
        addWeight(localWeight, deltaUpdates, scale);
        incrClock(clock);
    }

    protected void addWeight(float localWeight, int deltaUpdates, float scale) {
        assert (deltaUpdates > 0) : deltaUpdates;
        scaledSumWeights += ((localWeight / scale) * deltaUpdates);
        totalUpdates += deltaUpdates; // note deltaUpdates is in range (0,127]
        assert (totalUpdates > 0) : totalUpdates;
    }

    @Override
    public float getWeight(float scale) {
        return (float) (scaledSumWeights / totalUpdates) * scale;
    }

}
