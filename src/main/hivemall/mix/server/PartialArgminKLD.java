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

public final class PartialArgminKLD extends PartialResult {

    private final float scale;
    private volatile float sum_mean_div_covar;
    private volatile float sum_inv_covar;

    public PartialArgminKLD() {
        this(1.f); // no scaling
    }

    public PartialArgminKLD(float scale) {
        super();
        this.scale = scale;
        this.sum_mean_div_covar = 0.f;
        this.sum_inv_covar = 0.f;
    }

    @Override
    public synchronized void add(float localWeight, float covar, short clock) {
        addWeight(localWeight, covar);
        setMinCovariance(covar);
        incrClock(clock);
    }

    protected void addWeight(float localWeight, float covar) {
        this.sum_mean_div_covar += (localWeight / covar) / scale;
        this.sum_inv_covar += (1.f / covar) / scale;
    }

    @Override
    public float getWeight() {
        return sum_mean_div_covar / sum_inv_covar;
    }

}
