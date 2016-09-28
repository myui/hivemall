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

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.SingularValueDecomposition;
import org.junit.Assert;
import org.junit.Test;

public class MatrixUtilsTest {

    @Test
    public void testAryule1k2() {
        int k = 2;
        double[] R = new double[] {1, 2, 3};
        double[] A = new double[k + 1];
        MatrixUtils.aryule(R, A, k);
        Assert.assertEquals(1.3333333333333335d, A[1], 0.001d);
        Assert.assertEquals(0.3333333333333333d, A[2], 0.001d);
    }

    @Test
    public void testAryule1k9() {
        final int k = 9;
        double[] R = new double[] {143.85, 141.95, 141.45, 142.30, 140.60, 140.00, 138.40, 137.10,
                138.90, 139.85};
        double[] A = new double[k + 1];
        MatrixUtils.aryule(R, A, k);
        double[] expected = new double[] {-1.0, 2.90380682, 0.31235631, 1.26463104, -3.30187384,
                -1.61653593, -2.10367317, 1.37563117, 2.18139823, 0.02314717};
        Assert.assertArrayEquals(expected, A, 0.01d);
    }

    @Test
    public void testArburgk2() {
        // Expected outputs are obtained from Octave's arburg() function
        // k must be less than (X.length - 2) on Octave, but A can be computed w/o the assumption
        int k = 2;
        double[] X = new double[] {1, 2, 3, 4, 5};
        double[] A = new double[k + 1];
        MatrixUtils.arburg(X, A, k);
        Assert.assertEquals(-1.86391d, A[1], 0.00001d);
        Assert.assertEquals(0.95710d, A[2], 0.00001d);
    }

    @Test
    public void testArburgk5() {
        final int k = 5;
        double[] X = new double[] {143.85, 141.95, 141.45, 142.30, 140.60, 140.00, 138.40, 137.10,
                138.90, 139.85};
        double[] A = new double[k + 1];
        MatrixUtils.arburg(X, A, k);
        double[] expected = new double[] {-1.0, -1.31033, 0.58569, -0.56058, 0.63859, -0.35334};
        Assert.assertArrayEquals(expected, A, 0.00001d);
    }

    @Test
    public void testToeplitz() {
        RealMatrix[] c = new RealMatrix[] {new Array2DRowRealMatrix(new double[] {1}),
                new Array2DRowRealMatrix(new double[] {2}),
                new Array2DRowRealMatrix(new double[] {3})};
        RealMatrix[][] A = MatrixUtils.toeplitz(c, 3);
        // 1  2  3
        // 2  1  2
        // 3  2  1
        Assert.assertArrayEquals(new RealMatrix[] {new Array2DRowRealMatrix(new double[] {1}),
                new Array2DRowRealMatrix(new double[] {2}),
                new Array2DRowRealMatrix(new double[] {3})}, A[0]);
        Assert.assertArrayEquals(new RealMatrix[] {new Array2DRowRealMatrix(new double[] {2}),
                new Array2DRowRealMatrix(new double[] {1}),
                new Array2DRowRealMatrix(new double[] {2})}, A[1]);
        Assert.assertArrayEquals(new RealMatrix[] {new Array2DRowRealMatrix(new double[] {3}),
                new Array2DRowRealMatrix(new double[] {2}),
                new Array2DRowRealMatrix(new double[] {1})}, A[2]);
    }

    @Test
    public void testCombineMatrices1D() {
        RealMatrix[] m1 = new RealMatrix[] {new Array2DRowRealMatrix(new double[] {0, 1}),
                new Array2DRowRealMatrix(new double[] {2, 3}),
                new Array2DRowRealMatrix(new double[] {4, 5})};
        RealMatrix flatten1 = MatrixUtils.combinedMatrices(m1);
        double[][] data = flatten1.getData();
        Assert.assertEquals(6, data.length);
        Assert.assertArrayEquals(new double[] {0}, data[0], 0.d);
        Assert.assertArrayEquals(new double[] {1}, data[1], 0.d);
        Assert.assertArrayEquals(new double[] {2}, data[2], 0.d);
        Assert.assertArrayEquals(new double[] {3}, data[3], 0.d);
        Assert.assertArrayEquals(new double[] {4}, data[4], 0.d);
        Assert.assertArrayEquals(new double[] {5}, data[5], 0.d);
    }

    @Test
    public void testCombinedMatrices2D() {
        RealMatrix[] m1 = new RealMatrix[] {
                new Array2DRowRealMatrix(new double[][] {new double[] {1, 2, 3},
                        new double[] {4, 5, 6}}),
                new Array2DRowRealMatrix(new double[][] {new double[] {7, 8, 9},
                        new double[] {10, 11, 12}}),
                new Array2DRowRealMatrix(new double[][] {new double[] {13, 14, 15},
                        new double[] {16, 17, 18}})};
        RealMatrix flatten1 = MatrixUtils.combinedMatrices(m1);
        Assert.assertEquals(3, flatten1.getColumnDimension());
        Assert.assertEquals(6, flatten1.getRowDimension());
    }

    @Test
    public void testCombinedMatrices2D_Toeplitz() {
        RealMatrix e1 = new Array2DRowRealMatrix(new double[] {1, 1.1});
        RealMatrix e2 = new Array2DRowRealMatrix(new double[] {2, 2.2});
        RealMatrix e2T = e2.transpose();
        RealMatrix e3 = new Array2DRowRealMatrix(new double[] {3, 3.3});
        RealMatrix e3T = e3.transpose();
        RealMatrix[] m1 = new RealMatrix[] {e1, e2, e3};
        // {1.0,1.1}
        // {2.0,2.2}
        // {3.0,3.3}
        RealMatrix[][] toeplitz1 = MatrixUtils.toeplitz(m1, 3);
        Assert.assertEquals(3, toeplitz1.length);
        Assert.assertEquals(3, toeplitz1[0].length);
        Assert.assertEquals(3, toeplitz1[1].length);
        Assert.assertEquals(3, toeplitz1[2].length);
        Assert.assertEquals(e1, toeplitz1[0][0]);
        Assert.assertEquals(e1, toeplitz1[1][1]);
        Assert.assertEquals(e1, toeplitz1[2][2]);
        Assert.assertEquals(e2, toeplitz1[1][0]);
        Assert.assertEquals(e2, toeplitz1[2][1]);
        Assert.assertEquals(e3, toeplitz1[2][0]);
        Assert.assertEquals(e2T, toeplitz1[0][1]);
        Assert.assertEquals(e2T, toeplitz1[1][2]);
        Assert.assertEquals(e3T, toeplitz1[0][2]);
        // {1.0,1.1}  {2.0,2.2}' {3.0,3.3}'
        // {2.0,2.2}  {1.0,1.1}  {2.0,2.2}'
        // {3.0,3.3}  {2.0,2.2}  {1.0,1.1}
        RealMatrix flatten1 = MatrixUtils.combinedMatrices(toeplitz1, 2);
        // 1.0 0.0 2.0 2.2 3.0 3.3
        // 1.1 0.0 0.0 0.0 0.0 0.0
        // 2.0 0.0 1.0 0.0 2.0 2.2
        // 2.2 0.0 1.1 0.0 0.0 0.0
        // 3.0 0.0 2.0 0.0 1.0 0.0
        // 3.3 0.0 2.2 0.0 1.1 0.0
        Assert.assertEquals(6, flatten1.getRowDimension());
        Assert.assertEquals(6, flatten1.getColumnDimension());
    }

    @Test
    public void testFlatten1D() {
        RealMatrix[] m1 = new RealMatrix[] {new Array2DRowRealMatrix(new double[] {0, 1}),
                new Array2DRowRealMatrix(new double[] {2, 3}),
                new Array2DRowRealMatrix(new double[] {4, 5})};
        double[] actual = MatrixUtils.flatten(m1);
        Assert.assertArrayEquals(actual, new double[] {0, 1, 2, 3, 4, 5}, 0.d);
    }

    @Test
    public void testFlatten2D() {
        RealMatrix[] m1 = new RealMatrix[] {
                new Array2DRowRealMatrix(new double[][] {new double[] {1, 2, 3},
                        new double[] {4, 5, 6}}),
                new Array2DRowRealMatrix(new double[][] {new double[] {7, 8, 9},
                        new double[] {10, 11, 12}}),
                new Array2DRowRealMatrix(new double[][] {new double[] {13, 14, 15},
                        new double[] {16, 17, 18}})};
        double[] actual = MatrixUtils.flatten(m1);
        double[] expected = new double[18];
        for (int i = 0; i < expected.length; i++) {
            expected[i] = i + 1;
        }
        Assert.assertArrayEquals(expected, actual, 0.d);
    }

    @Test
    public void testUnflatten2D() {
        double[] data = new double[24];
        for (int i = 0; i < data.length; i++) {
            data[i] = i + 1;
        }
        RealMatrix[] actual = MatrixUtils.unflatten(data, 2, 3, 4);

        RealMatrix[] expected = new RealMatrix[] {
                new Array2DRowRealMatrix(new double[][] {new double[] {1, 2, 3},
                        new double[] {4, 5, 6}}),
                new Array2DRowRealMatrix(new double[][] {new double[] {7, 8, 9},
                        new double[] {10, 11, 12}}),
                new Array2DRowRealMatrix(new double[][] {new double[] {13, 14, 15},
                        new double[] {16, 17, 18}}),
                new Array2DRowRealMatrix(new double[][] {new double[] {19, 20, 21},
                        new double[] {22, 23, 24}})};

        Assert.assertArrayEquals(expected, actual);
    }

    @Test
    public void testPower1() {
        RealMatrix A = new Array2DRowRealMatrix(new double[][] {new double[] {1, 2, 3}, new double[] {4, 5, 6}});

        double[] x = new double[3];
        x[0] = Math.random();
        x[1] = Math.random();
        x[2] = Math.random();

        double[] u = new double[2];
        double[] v = new double[3];

        double s = MatrixUtils.power1(A, x, 2, u, v);

        SingularValueDecomposition svdA = new SingularValueDecomposition(A);

        Assert.assertArrayEquals(svdA.getU().getColumn(0), u, 0.001d);
        Assert.assertArrayEquals(svdA.getV().getColumn(0), v, 0.001d);
        Assert.assertEquals(svdA.getSingularValues()[0], s, 0.001d);
    }

    @Test
    public void testLanczosTridiagonalization() {
        // Symmetric matrix
        RealMatrix C = new Array2DRowRealMatrix(new double[][] {
                                new double[] {1, 2, 3, 4}, new double[] {2, 1, 4, 3},
                                new double[] {3, 4, 1, 2}, new double[] {4, 3, 2, 1}});

        // naive initial vector
        double[] a = new double[] {1, 1, 1, 1};

        RealMatrix actual = new Array2DRowRealMatrix(new double[4][4]);
        MatrixUtils.lanczosTridiagonalization(C, a, actual);

        RealMatrix expected = new Array2DRowRealMatrix(new double[][] {
                                new double[] {40, 60, 0, 0}, new double[] {60, 10, 120, 0},
                                new double[] {0, 120, 10, 120}, new double[] {0, 0, 120, 10}});

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testTridiagonalEigen() {
        // Tridiagonal Matrix
        RealMatrix T = new Array2DRowRealMatrix(new double[][] {
                new double[] {40, 60, 0, 0}, new double[] {60, 10, 120, 0},
                new double[] {0, 120, 10, 120}, new double[] {0, 0, 120, 10}});

        double[] eigvals = new double[4];
        RealMatrix eigvecs = new Array2DRowRealMatrix(new double[4][4]);

        MatrixUtils.tridiagonalEigen(T, 2, eigvals, eigvecs);

        RealMatrix actual = eigvecs.multiply(eigvecs.transpose());

        RealMatrix expected = new Array2DRowRealMatrix(new double[4][4]);
        for (int i = 0; i < 4; i++) {
            expected.setEntry(i, i, 1);
        }

        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 4; j++) {
                Assert.assertEquals(expected.getEntry(i, j), actual.getEntry(i, j), 0.001d);
            }
        }
    }
}
