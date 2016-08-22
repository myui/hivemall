/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
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
package hivemall.anomaly;

import hivemall.utils.lang.Preconditions;
import hivemall.utils.math.MatrixUtils;

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

    private double _mu;
    private double[] _C;
    private double[] _A;
    private double _sigma;

    public SDAR1D(final double r, final int k) {
        Preconditions.checkArgument(0.d < r && r < 1.d, "Invalid forgetfullness parameter r: " + r);
        Preconditions.checkArgument(k >= 1, "Invalid smoothing parameter k: " + k);
        this._r = r;
        this._C = new double[k + 1];
        this._A = new double[k + 1];
    }

    /**
     * @param x series of input in LIFO order
     */
    public double update(@Nonnull final double[] x) {
        Preconditions.checkArgument(x.length >= 1, "x.length MUST be greather than 1: " + x.length);
        final int k = x.length - 1;
        Preconditions.checkArgument(k < _C.length,
            "x.length MUST be less than smooting window size: " + k);

        final double x_t = x[0];
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
        MatrixUtils.aryule(C, A, k);

        // estimate x
        // \hat{x} = \hat{µ} + ∑_{i=1}^k A_i (x_{t-i} - \hat{µ})
        double x_hat = _mu;
        for (int i = 1; i <= k; i++) {
            x_hat += (-A[i]) * (x[i] - _mu); // -A[i] when solved by aryule()
        }

        // update model covariance
        // ∑ := (1-r) ∑ + r (x - \hat{x}) (x - \hat{x})'
        this._sigma = (1.d - _r) * _sigma + _r * (x_t - x_hat) * (x_t - x_hat);
        
        return x_hat;
    }

    public double logLoss(final double actual, final double predicted) {
        double p = pdf(actual, predicted, _mu, _sigma);
        return -Math.log(p);
    }

    /**
     * @return value of probabilistic density function
     */
    private static double pdf(final double x, final double x_hat, final double mu,
            final double sigma) {
        if (sigma == 0.d) {
            return 0.d;
        }
        double diff = x - x_hat;
        double numerator = Math.exp(-0.5d * diff * diff / sigma);
        double denominator = Math.sqrt(2.d * Math.PI) * Math.sqrt(sigma);
        return numerator / denominator;
    }

}
