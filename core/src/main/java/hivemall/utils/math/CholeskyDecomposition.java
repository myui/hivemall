/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
 * Copyright (c) 2010 Haifeng Li
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

import java.util.Arrays;

import javax.annotation.Nonnull;

import smile.math.Math;

/**
 * Cholesky decomposition is a decomposition of a symmetric, positive-definite matrix into a lower
 * triangular matrix L and the transpose of the lower triangular matrix such that A = L*L'.
 * 
 * If the matrix is not symmetric or positive definite, the constructor returns a partial
 * decomposition and sets an internal flag that may be queried by the isSPD() method.
 * 
 * Based on the <code>smile.math.matrix.CholeskyDecomposition</code>.
 */
public final class CholeskyDecomposition {

    /**
     * Array for internal storage of decomposition.
     */
    private final double[][] _L;

    /**
     * Symmetric and positive definite flag.
     * 
     * @serial is symmetric and positive definite flag.
     */
    private final boolean _isSPD;

    /**
     * Constructor. Cholesky decomposition for symmetric and positive definite matrix. A new matrix
     * will allocated to store the decomposition.
     * 
     * @param A square, symmetric matrix. Only the lower triangular part will be used in the
     *        decomposition. The user can just store this half part to save space.
     * @throws IllegalArgumentException if the matrix is not positive definite.
     */
    public CholeskyDecomposition(double[][] A) {
        this(A, false);
    }

    /**
     * Constructor. Cholesky decomposition for symmetric and positive definite matrix. The user can
     * specify if the decomposition takes in place, i.e. if the decomposition will be stored in the
     * input matrix. Otherwise, a new matrix will be allocated to store the decomposition.
     *
     * @param A square symmetric matrix. Only the lower triangular part will be used in the
     *        decomposition. The user can just store this half part to save space.
     * @param overwrite true if the decomposition will be taken in place. Otherwise, a new matrix
     *        will be allocated to store the decomposition. It is very useful in practice if the
     *        matrix is huge.
     * @throws IllegalArgumentException if the matrix is not positive definite.
     */
    public CholeskyDecomposition(double[][] A, boolean overwrite) {
        int n = A.length;
        if (n != A[n - 1].length) {
            throw new IllegalArgumentException("The matrix is not square.");
        }

        final double[][] L;
        if (overwrite) {
            L = A;
        } else {
            L = new double[n][];
            for (int i = 0; i < n; i++) {
                L[i] = new double[i + 1];
            }
        }

        // Main loop.
        boolean spd = true;
        for (int j = 0; j < n; j++) {
            double[] Lrowj = L[j];
            double d = 0.0;
            for (int k = 0; k < j; k++) {
                double[] Lrowk = L[k];
                double s = 0.0;
                for (int i = 0; i < k; i++) {
                    s += Lrowk[i] * Lrowj[i];
                }
                Lrowj[k] = s = (A[j][k] - s) / L[k][k];
                d = d + s * s;
            }
            d = A[j][j] - d;

            if (d <= 0.0) {
                spd = false;
                d = Double.MIN_VALUE; // smallest positive nonzero value
            }

            L[j][j] = Math.sqrt(d);

            if (L == A && L[j].length == n) {
                Arrays.fill(L[j], j + 1, n, 0.0);
            }
        }
        this._L = L;
        this._isSPD = spd;
    }

    @Nonnull
    public static double[][] getL(@Nonnull final double[][] A, final boolean overwrite,
            final boolean partialDecomposition) {
        final int n = A.length;
        if (n != A[n - 1].length) {
            throw new IllegalArgumentException("The matrix is not square.");
        }

        final double[][] L;
        if (overwrite) {
            L = A;
        } else {
            L = new double[n][n];
        }

        // Main loop.
        for (int j = 0; j < n; j++) {
            final double[] Lj = L[j];
            final double[] Aj = A[j];
            double d = 0.0;
            for (int k = 0; k < j; k++) {
                double[] Lk = L[k];
                double s = 0.0;
                for (int i = 0; i < k; i++) {
                    s += Lk[i] * Lj[i];
                }
                Lj[k] = s = (Aj[k] - s) / Lk[k];
                d = d + s * s;
            }
            d = Aj[j] - d;

            if (d <= 0.0) {
                if (partialDecomposition) {
                    d = Double.MIN_VALUE; // smallest positive nonzero value
                } else {
                    throw new IllegalArgumentException("The matrix is not positive definite: " + d);
                }
            }

            Lj[j] = Math.sqrt(d);

            if (overwrite) {
                Arrays.fill(Lj, j + 1, n, 0.0);
            }
        }

        return L;
    }

    /**
     * Is the matrix symmetric and positive definite?
     * 
     * @return true if A is symmetric and positive definite.
     */
    public boolean isSPD() {
        return _isSPD;
    }

    /**
     * Returns lower triangular factor.
     */
    public double[][] getL() {
        return _L;
    }

    /**
     * Returns the matrix determinant
     */
    public double det() {
        double d = 1.0;
        for (int i = 0; i < _L.length; i++) {
            d *= _L[i][i];
        }

        return d * d;
    }

    /**
     * Returns the matrix inverse.
     */
    public double[][] inverse() {
        double[][] I = Math.eye(_L.length);
        solve(I);
        return I;
    }

    /**
     * Solve the linear system A * x = b. On output, b will be overwritten with the solution vector.
     * 
     * @param b the right hand side of linear systems. On output, b will be overwritten with
     *        solution vector.
     */
    public void solve(double[] b) {
        solve(b, b);
    }

    /**
     * Solve the linear system A * x = b.
     * 
     * @param b the right hand side of linear systems.
     * @param x the solution vector.
     */
    public void solve(double[] b, double[] x) {
        if (b.length != _L.length) {
            throw new IllegalArgumentException(String.format(
                "Row dimensions do not agree: A is %d x %d, but B is %d x 1", _L.length, _L.length,
                b.length));
        }

        if (b.length != x.length) {
            throw new IllegalArgumentException("b and x dimensions do not agree.");
        }

        int n = b.length;

        if (x != b) {
            System.arraycopy(b, 0, x, 0, n);
        }

        // Solve L*Y = B;
        for (int k = 0; k < n; k++) {
            for (int i = 0; i < k; i++) {
                x[k] -= x[i] * _L[k][i];
            }
            x[k] /= _L[k][k];
        }

        // Solve L'*X = Y;
        for (int k = n - 1; k >= 0; k--) {
            for (int i = k + 1; i < n; i++) {
                x[k] -= x[i] * _L[i][k];
            }
            x[k] /= _L[k][k];
        }
    }

    /**
     * Solve the linear system A * X = B. On output, B will be overwritten with the solution matrix.
     * 
     * @param B the right hand side of linear systems.
     */
    public void solve(double[][] B) {
        solve(B, B);
    }

    /**
     * Solve the linear system A * X = B.
     * 
     * @param B the right hand side of linear systems.
     * @param X the solution matrix.
     */
    public void solve(double[][] B, double[][] X) {
        if (B.length != _L.length) {
            throw new IllegalArgumentException(String.format(
                "Row dimensions do not agree: A is %d x %d, but B is %d x %d", _L.length,
                _L.length, B.length, B[0].length));
        }

        if (X.length != B.length || X[0].length != B[0].length) {
            throw new IllegalArgumentException("B and X dimensions do not agree.");
        }

        int n = B.length;
        int nx = B[0].length;

        if (X != B) {
            for (int i = 0; i < n; i++) {
                System.arraycopy(B[i], 0, X[i], 0, nx);
            }
        }

        // Solve L*Y = B;
        for (int k = 0; k < n; k++) {
            for (int j = 0; j < nx; j++) {
                for (int i = 0; i < k; i++) {
                    X[k][j] -= X[i][j] * _L[k][i];
                }
                X[k][j] /= _L[k][k];
            }
        }

        // Solve L'*X = Y;
        for (int k = n - 1; k >= 0; k--) {
            for (int j = 0; j < nx; j++) {
                for (int i = k + 1; i < n; i++) {
                    X[k][j] -= X[i][j] * _L[i][k];
                }
                X[k][j] /= _L[k][k];
            }
        }
    }
}
