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

/**
 * @see http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
 */
public final class OnlineVariance {

    private long n;
    private double mean;
    private double m2;

    public OnlineVariance() {
        reset();
    }

    public void reset() {
        this.n = 0L;
        this.mean = 0.d;
        this.m2 = 0.d;
    }

    public void handle(double x) {
        ++n;
        double delta = x - mean;
        mean += delta / n;
        m2 += delta * (x - mean);
    }

    public void unhandle(double x) {
        if(n == 0L) {
            return; // nop
        }
        if(n == 1L) {
            reset();
            return;
        }
        double old_mean = (n * mean - x) / (n - 1L);
        m2 -= (x - mean) * (x - old_mean);
        mean = old_mean;
        --n;
    }

    public long numSamples() {
        return n;
    }

    public double mean() {
        return mean;
    }

    public double variance() {
        return n > 1 ? (m2 / (n - 1)) : 0.d;
    }

    public double stddev() {
        return Math.sqrt(variance());
    }

}
