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
