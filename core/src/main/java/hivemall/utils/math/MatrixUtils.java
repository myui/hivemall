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
package hivemall.utils.math;

import hivemall.utils.lang.Preconditions;

import javax.annotation.Nonnull;

import org.apache.commons.math3.linear.BlockRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;

public final class MatrixUtils {

    private MatrixUtils() {}

    /**
     * Solve Yule-walker equation by Levinson-Durbin Recursion.
     * 
     * R_j = ∑_{i=1}^{k} A_i R_{j-i} where j = 1..k, R_{-i} = R'_i
     * 
     * @param R autocovariance where |R| >= order
     * @param A coefficient to be solved where |A| >= order + 1
     * @return E variance of prediction error
     */
    @Nonnull
    public static double[] aryule(@Nonnull final double[] R, @Nonnull final double[] A,
            final int order) {
        Preconditions.checkArgument(R.length > order, "|R| MUST be greater than or equals to "
                + order + ": " + R.length);
        Preconditions.checkArgument(A.length >= order + 1, "|A| MUST be greater than or equals to "
                + (order + 1) + ": " + A.length);

        final double[] E = new double[order + 1];
        A[0] = 1.0d;
        E[0] = R[0];
        for (int k = 0; k < order; k++) {
            double lambda = 0.d;
            for (int j = 0; j <= k; j++) {
                lambda -= A[j] * R[k + 1 - j];
            }
            final double Ek = E[k];
            if (Ek == 0.d) {
                lambda = 0.d;
            } else {
                lambda /= Ek;
            }

            for (int n = 0, last = (k + 1) / 2; n <= last; n++) {
                final int i = k + 1 - n;
                double tmp = A[i] + lambda * A[n];
                A[n] += lambda * A[i];
                A[i] = tmp;
            }

            E[k + 1] = Ek * (1.0d - lambda * lambda);
        }

        for (int i = 0; i < order + 1; i++) {
            A[i] = -A[i];
        }

        return E;
    }

    @Deprecated
    @Nonnull
    public static double[] aryule2(@Nonnull final double[] R, @Nonnull final double[] A,
            final int order) {
        Preconditions.checkArgument(R.length > order, "|C| MUST be greater than or equals to "
                + order + ": " + R.length);
        Preconditions.checkArgument(A.length >= order + 1, "|A| MUST be greater than or equals to "
                + (order + 1) + ": " + A.length);

        final double[] E = new double[order + 1];
        A[0] = E[0] = 1.0d;
        A[1] = -R[1] / R[0];
        E[1] = R[0] + R[1] * A[1];
        for (int k = 1; k < order; k++) {
            double lambda = 0.d;
            for (int j = 0; j <= k; j++) {
                lambda -= A[j] * R[k + 1 - j];
            }
            lambda /= E[k];

            final double[] U = new double[k + 2];
            final double[] V = new double[k + 2];
            U[0] = 1.0; // V[0] = 0.0;
            for (int i = 1; i <= k; i++) {
                U[i] = A[i];
                V[k + 1 - i] = A[i];
            }
            V[k + 1] = 1.0; // U[k + 1] = 0.0;            
            for (int i = 0, threshold = k + 2; i < threshold; i++) {
                A[i] = U[i] + lambda * V[i];
            }

            E[k + 1] = E[k] * (1.0d - lambda * lambda);
        }

        for (int i = 0; i < order + 1; i++) {
            A[i] = -A[i];
        }

        return E;
    }

    /**
     * Fit an AR(order) model using the Burg's method.
     *
     * cf. https://searchcode.com/codesearch/view/9503568/
     *
     * @param X data vector to estimate where |X| >= order
     * @param A coefficient to be solved where |A| >= order + 1
     * @return E variance of white noise
     */
    @Nonnull
    public static double[] arburg(@Nonnull final double[] X, @Nonnull final double[] A,
                                  final int order) {
        Preconditions.checkArgument(X.length > order, "|X| MUST be greater than or equals to "
                + order + ": " + X.length);
        Preconditions.checkArgument(A.length >= order + 1, "|A| MUST be greater than or equals to "
                + (order + 1) + ": " + A.length);

        final int nDataPoints = X.length;
        final double[] E = new double[order + 1];

        E[0] = 0.0d;
        for (int i = 0; i < nDataPoints; i++) {
            E[0] += X[i] * X[i];
        }

        // f and b are the forward and backward error sequences
        int currentErrorSequenceSize = nDataPoints - 1;
        double[] F = new double[currentErrorSequenceSize];
        double[] B = new double[currentErrorSequenceSize];
        for (int i = 0; i < currentErrorSequenceSize; i++) {
            F[i] = X[i + 1];
            B[i] = X[i];
        }

        A[0] = 1.0d;

        // remaining stages i=2 to p
        for (int i = 0; i < order; i++) {

            // get the i-th reflection coefficient
            double numerator = 0.0d;
            double denominator = 0.0d;
            for (int j = 0; j < currentErrorSequenceSize; j++) {
                numerator += F[j] * B[j];
                denominator += F[j] * F[j] + B[j] * B[j];
            }
            numerator *= 2.0d;
            double g = 0.0d;
            if (denominator != 0.0d) {
                g = numerator / denominator;
            }

            // generate next filter order
            final double[] prevA = new double[i];
            for (int j = 0; j < i; j++) {
                // No need to copy A[0] = 1.0
                prevA[j] = A[j + 1];
            }
            A[1] = g;
            for (int j = 1; j < i + 1; j++) {
                A[j + 1] = prevA[j - 1] - g * prevA[i - j];
            }

            // keep track of the error
            E[i + 1] = E[i] * (1 - g * g);

            // update the prediction error sequences
            final double[] prevF = new double[currentErrorSequenceSize];
            for (int j = 0; j < currentErrorSequenceSize; j++) {
                prevF[j] = F[j];
            }
            final int nextErrorSequenceSize = nDataPoints - i - 2;
            for (int j = 0; j < nextErrorSequenceSize; j++) {
                F[j] = prevF[j + 1] - g * B[j + 1];
                B[j] = B[j] - g * prevF[j];
            }

            currentErrorSequenceSize = nextErrorSequenceSize;

        }

        for (int i = 1, mid = order / 2 + 1; i < mid; i++) {
            // Reverse 1..(order - 1)-th elements by swapping
            final double tmp = A[i];
            A[i] = A[order + 1 - i];
            A[order + 1 - i] = tmp;
        }

        for (int i = 0; i < order + 1; i++) {
            A[i] = -A[i];
        }

        return E;
    }

    /**
     * Construct a Toeplitz matrix.
     */
    @Nonnull
    public static RealMatrix[][] toeplitz(@Nonnull final RealMatrix[] c, final int dim) {
        Preconditions.checkArgument(dim >= 1, "Invliad dimension: " + dim);
        Preconditions.checkArgument(c.length >= dim, "|c| must be greather than " + dim + ": "
                + c.length);

        /*
         * Toeplitz matrix  (symmetric, invertible, k*dimensions by k*dimensions)
         *
         * /C_0     |C_1     |C_2     | .  .  .   |C_{k-1} \
         * |--------+--------+--------+           +--------|
         * |C_1'    |C_0     |C_1     |               .    |
         * |--------+--------+--------+               .    |
         * |C_2'    |C_1'    |C_0     |               .    |
         * |--------+--------+--------+                    |
         * |   .                         .                 |
         * |   .                            .              |
         * |   .                               .           |
         * |--------+                              +-------|
         * \C_{k-1}'| .  .  .                      |C_0    /
         */
        final RealMatrix c0 = c[0];
        final RealMatrix[][] toeplitz = new RealMatrix[dim][dim];
        for (int row = 0; row < dim; row++) {
            toeplitz[row][row] = c0;
            for (int col = 0; col < dim; col++) {
                if (row < col) {
                    toeplitz[row][col] = c[col - row];
                } else if (row > col) {
                    toeplitz[row][col] = c[row - col].transpose();
                }
            }
        }
        return toeplitz;
    }

    @Nonnull
    public static RealMatrix flatten(@Nonnull final RealMatrix[][] grid) {
        Preconditions.checkArgument(grid.length >= 1, "The number of rows must be greather than 1");
        Preconditions.checkArgument(grid[0].length >= 1,
            "The number of cols must be greather than 1");

        final int rows = grid.length;
        final int cols = grid[0].length;
        final int rowDims = grid[0][0].getRowDimension();
        final int colDims = grid[0][0].getColumnDimension();

        final RealMatrix combined = new BlockRealMatrix(rows * rowDims, cols * colDims);
        for (int row = 0; row < grid.length; row++) {
            for (int col = 0; col < grid[row].length; col++) {
                combined.setSubMatrix(grid[row][col].getData(), row * rowDims, col * colDims);
            }
        }
        return combined;
    }

    /**
     * Flatten grid of matrix that has 1 col.
     */
    @Nonnull
    public static RealMatrix flatten(@Nonnull final RealMatrix[] grid) {
        Preconditions.checkArgument(grid.length >= 1,
            "The number of rows must be greather than 0: " + grid.length);

        final int rows = grid.length;
        final int rowDims = grid[0].getRowDimension();
        final int colDims = grid[0].getColumnDimension();

        final RealMatrix combined = new BlockRealMatrix(rows * rowDims, colDims);
        for (int row = 0; row < grid.length; row++) {
            combined.setSubMatrix(grid[row].getData(), row * rowDims, 0);
        }
        return combined;
    }

}
