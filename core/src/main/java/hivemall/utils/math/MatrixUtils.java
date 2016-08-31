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
        for (int i = 0; i < dim; i++) {
            toeplitz[i][i] = c0;
            for (int j = 0; j < i; j++) {
                int index = i - j - 1;
                assert (index >= 0) : index;
                toeplitz[i][j] = c[index].transpose();
                toeplitz[j][i] = c[index];
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
        Preconditions.checkArgument(grid.length >= 1, "The number of rows must be greather than 1");

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
