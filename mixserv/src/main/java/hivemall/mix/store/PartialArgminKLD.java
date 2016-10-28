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

import javax.annotation.Nonnegative;
import javax.annotation.concurrent.GuardedBy;

public final class PartialArgminKLD extends PartialResult {

    @GuardedBy("lock()")
    private double sum_mean_div_covar;
    @GuardedBy("lock()")
    private float sum_inv_covar;

    public PartialArgminKLD() {
        super();
        this.sum_mean_div_covar = 0.f;
        this.sum_inv_covar = 0.f;
    }

    @Override
    public float getCovariance(float scale) {
        return 1.f / (sum_inv_covar * scale);
    }

    @Override
    public void add(float localWeight, float covar, int deltaUpdates, float scale) {
        assert (deltaUpdates > 0) : deltaUpdates;
        addWeight(localWeight, covar, scale);
        incrClock(deltaUpdates);
    }

    private void addWeight(float localWeight, float covar, float scale) {
        this.sum_mean_div_covar += (localWeight / covar) / scale;
        this.sum_inv_covar += (1.f / covar) / scale;
    }

    @Override
    public void subtract(float localWeight, float covar, @Nonnegative int deltaUpdates, float scale) {
        this.sum_mean_div_covar -= (localWeight / covar) / scale;
        this.sum_inv_covar -= (1.f / covar) / scale;
    }

    @Override
    public float getWeight(float scale) {
        return (float) (sum_mean_div_covar / sum_inv_covar);
    }

}
