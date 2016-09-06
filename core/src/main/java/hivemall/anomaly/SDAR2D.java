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
import hivemall.utils.math.StatsUtils;

import java.util.Arrays;

import javax.annotation.Nonnull;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.BlockRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;

public final class SDAR2D {

    // --------------------------------
    // hyperparameters

    /**
     * Forgetfulness parameter
     */
    private final double _r;

    // --------------------------------
    // parameters

    private final RealMatrix[] _C;
    private RealVector _mu;
    private RealMatrix _sigma;
    private RealVector _muOld;
    private RealMatrix _sigmaOld;

    private boolean _initialized;

    public SDAR2D(final double r, final int k) {
        Preconditions.checkArgument(0.d < r && r < 1.d, "Invalid forgetfullness parameter r: " + r);
        Preconditions.checkArgument(k >= 1, "Invalid smoothing parameter k: " + k);
        this._r = r;
        this._C = new RealMatrix[k + 1];
        this._initialized = false;
    }

    /**
     * @param x series of input in LIFO order
     * @param k AR window size
     * @return x_hat predicted x
     * @link https://en.wikipedia.org/wiki/Matrix_multiplication#Outer_product
     */
    @Nonnull
    public RealVector update(@Nonnull final ArrayRealVector[] x, final int k) {
        Preconditions.checkArgument(x.length >= 1, "x.length MUST be greather than 1: " + x.length);
        Preconditions.checkArgument(k >= 0, "k MUST be greather than or equals to 0: ", k);
        Preconditions.checkArgument(k < _C.length, "k MUST be less than |C| but " + "k=" + k
                + ", |C|=" + _C.length);

        final ArrayRealVector x_t = x[0];
        final int dims = x_t.getDimension();

        if (_initialized == false) {
            this._mu = x_t.copy();
            this._sigma = new BlockRealMatrix(dims, dims);
            assert (_sigma.isSquare());
            this._initialized = true;
            return new ArrayRealVector(dims);
        }
        Preconditions.checkArgument(k >= 1, "k MUST be greater than 0: ", k);

        // old parameters are accessible to compute the Hellinger distance
        this._muOld = _mu.copy();
        this._sigmaOld = _sigma.copy();

        // update mean vector
        // \hat{mu} := (1-r) \hat{µ} + r x_t
        this._mu = _mu.mapMultiply(1.d - _r).add(x_t.mapMultiply(_r));

        // compute residuals (x - \hat{µ})
        final RealVector[] xResidual = new RealVector[k + 1];
        for (int j = 0; j <= k; j++) {
            xResidual[j] = x[j].subtract(_mu);
        }

        // update covariance matrices
        // C_j := (1-r) C_j + r (x_t - \hat{µ}) (x_{t-j} - \hat{µ})'
        final RealMatrix[] C = this._C;
        final RealVector rxResidual0 = xResidual[0].mapMultiply(_r); // r (x_t - \hat{µ}) 
        for (int j = 0; j <= k; j++) {
            RealMatrix Cj = C[j];
            if (Cj == null) {
                C[j] = rxResidual0.outerProduct(x[j].subtract(_mu));
            } else {
                C[j] = Cj.scalarMultiply(1.d - _r)
                         .add(rxResidual0.outerProduct(x[j].subtract(_mu)));
            }
        }

        // solve A in the following Yule-Walker equation
        // C_j = ∑_{i=1}^{k} A_i C_{j-i} where j = 1..k, C_{-i} = C_i' 
        /*
         * /C_1\     /A_1\  /C_0     |C_1'    |C_2'    | .  .  .   |C_{k-1}' \ 
         * |---|     |---|  |--------+--------+--------+           +---------|
         * |C_2|     |A_2|  |C_1     |C_0     |C_1'    |               .     |
         * |---|     |---|  |--------+--------+--------+               .     |
         * |C_3|  =  |A_3|  |C_2     |C_1     |C_0     |               .     |
         * | . |     | . |  |--------+--------+--------+                     |
         * | . |     | . |  |   .                            .               |
         * | . |     | . |  |   .                            .               |
         * |---|     |---|  |--------+                              +--------|
         * \C_k/     \A_k/  \C_{k-1} | .  .  .                      |C_0     / 
         */
        RealMatrix[][] rhs = MatrixUtils.toeplitz(C, k);
        RealMatrix[] lhs = Arrays.copyOfRange(C, 1, k + 1);
        RealMatrix R = MatrixUtils.combinedMatrices(rhs, dims);
        RealMatrix L = MatrixUtils.combinedMatrices(lhs);
        RealMatrix A = MatrixUtils.solve(L, R, false);

        // estimate x
        // \hat{x} = \hat{µ} + ∑_{i=1}^k A_i (x_{t-i} - \hat{µ})
        RealVector x_hat = _mu.copy();
        for (int i = 0; i < k; i++) {
            int offset = i * dims;
            RealMatrix Ai = A.getSubMatrix(offset, offset + dims - 1, 0, dims - 1);
            x_hat = x_hat.add(Ai.operate(xResidual[i + 1]));
        }

        // update model covariance
        // ∑ := (1-r) ∑ + r (x - \hat{x}) (x - \hat{x})'       
        RealVector xEstimateResidual = x_t.subtract(x_hat);
        this._sigma = _sigma.scalarMultiply(1.d - _r).add(
            xEstimateResidual.mapMultiply(_r).outerProduct(xEstimateResidual));

        return x_hat;
    }

    public double logLoss(@Nonnull final RealVector actual, final RealVector predicted) {
        return StatsUtils.logLoss(actual, predicted, _sigma);
    }

    public double hellingerDistance() {
        return StatsUtils.hellingerDistance(_muOld, _sigmaOld, _mu, _sigma);
    }
}
