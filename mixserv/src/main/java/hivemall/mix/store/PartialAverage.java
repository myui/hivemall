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
    public void add(float localWeight, float covar, @Nonnegative int deltaUpdates, float scale) {
        assert (deltaUpdates > 0) : deltaUpdates;
        addWeight(localWeight, deltaUpdates, scale);
        incrClock(deltaUpdates);
    }

    private void addWeight(float localWeight, int deltaUpdates, float scale) {
        assert (deltaUpdates > 0) : deltaUpdates;
        scaledSumWeights += ((localWeight / scale) * deltaUpdates);
        totalUpdates += deltaUpdates; // note deltaUpdates is in range (0,127]
        assert (totalUpdates > 0) : totalUpdates;
    }

    @Override
    public void subtract(float localWeight, float covar, @Nonnegative int deltaUpdates, float scale) {
        assert (deltaUpdates > 0) : deltaUpdates;
        scaledSumWeights -= ((localWeight / scale) * deltaUpdates);
        totalUpdates -= deltaUpdates; // note deltaUpdates is in range (0,127]
        assert (totalUpdates > 0) : totalUpdates;
    }

    @Override
    public float getWeight(float scale) {
        return (float) (scaledSumWeights / totalUpdates) * scale;
    }

}
