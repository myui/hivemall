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
package hivemall.anomaly;

import hivemall.utils.lang.Preconditions;
import hivemall.utils.math.MatrixUtils;
import hivemall.utils.math.StatsUtils;

import java.util.Arrays;

import javax.annotation.Nonnull;

public final class SDAR1D {

    // --------------------------------
    // hyperparameters

    /**
     * Forgetfulness parameter
     */
    private final double _r;

    // --------------------------------
    // parameters

    private final double[] _C;
    private final double[] _A;
    private double _mu;
    private double _sigma;
    private double _muOld;
    private double _sigmaOld;

    private boolean _initialized;

    public SDAR1D(final double r, final int k) {
        Preconditions.checkArgument(0.d < r && r < 1.d, "Invalid forgetfullness parameter r: " + r);
        Preconditions.checkArgument(k >= 1, "Invalid smoothing parameter k: " + k);
        this._r = r;
        this._C = new double[k + 1];
        this._A = new double[k + 1];
        this._initialized = false;
    }

    /**
     * @param x series of input in LIFO order
     * @param k AR window size
     * @return x_hat predicted x
     */
    public double update(@Nonnull final double[] x, final int k) {
        Preconditions.checkArgument(x.length >= 1, "x.length MUST be greather than 1: ", x.length);
        Preconditions.checkArgument(k >= 0, "k MUST be greather than or equals to 0: ", k);
        Preconditions.checkArgument(k < _C.length, "k MUST be less than |C| but ", "k=", k
                + ", |C|=", _C.length);

        final double x_t = x[0];

        if (_initialized == false) {
            this._mu = x_t;
            this._sigma = 0.d;
            this._initialized = true;
            return 0.d;
        }
        Preconditions.checkArgument(k >= 1, "k MUST be greater than 0: ", k);

        // old parameters are accessible to compute the Hellinger distance
        this._muOld = _mu;
        this._sigmaOld = _sigma;

        // update mean vector
        // \hat{mu} := (1-r) \hat{µ} + r x_t
        this._mu = (1.d - _r) * _mu + _r * x_t;

        // update covariance matrices
        // C_j := (1-r) C_j + r (x_t - \hat{µ}) (x_{t-j} - \hat{µ})^T
        final double[] C = this._C;
        for (int j = 0; j <= k; j++) {
            C[j] = (1.d - _r) * C[j] + _r * (x_t - _mu) * (x[j] - _mu);
        }

        // solve A in the following Yule-Walker equation
        // C_j = ∑_{i=1}^{k} A_i C_{j-i} where j = 1..k, C_{-i} = C'_i
        final double[] A = _A;
        Arrays.fill(A, 0.d);
        MatrixUtils.aryule(C, A, k);

        // estimate x
        // \hat{x} = \hat{µ} + ∑_{i=1}^k A_i (x_{t-i} - \hat{µ})
        double x_hat = _mu;
        for (int i = 1; i <= k; i++) {
            x_hat += A[i] * (x[i] - _mu);
        }

        // update model covariance
        // ∑ := (1-r) ∑ + r (x - \hat{x}) (x - \hat{x})'
        double diffx = x_t - x_hat;
        this._sigma = (1.d - _r) * _sigma + _r * diffx * diffx;

        return x_hat;
    }

    public double logLoss(final double actual, final double predicted) {
        return StatsUtils.logLoss(actual, predicted, _sigma);
    }

    public double hellingerDistance() {
        return StatsUtils.hellingerDistance(_muOld, _sigmaOld, _mu, _sigma);
    }
}
