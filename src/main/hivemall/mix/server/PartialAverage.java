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

public final class PartialAverage extends PartialResult {
    public static final float DEFAULT_SCALE = 10;

    private final float scale;
    private float scaledSumWeights;
    private short totalUpdates;

    public PartialAverage() {
        this(1.f); // no scaling
    }

    public PartialAverage(float scale) {
        super();
        this.scale = scale;
        this.scaledSumWeights = 0.f;
        this.totalUpdates = 0;
    }

    @Override
    public void add(float localWeight, float covar, short clock, int deltaUpdates) {
        addWeight(localWeight, deltaUpdates);
        setMinCovariance(covar);
        incrClock(clock);
    }

    protected void addWeight(float localWeight, int deltaUpdates) {
        scaledSumWeights += ((localWeight / scale) * deltaUpdates);
        totalUpdates += deltaUpdates; // not deltaUpdates is in range (0,127]
        assert (totalUpdates > 0) : totalUpdates;
    }

    @Override
    public float getWeight() {
        return (scaledSumWeights / totalUpdates) * scale;
    }

}
