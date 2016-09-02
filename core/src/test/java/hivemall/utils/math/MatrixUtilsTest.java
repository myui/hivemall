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
    public void toeplitz() {
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
                new Array2DRowRealMatrix(new double[] {1}),
                new Array2DRowRealMatrix(new double[] {2})}, A[2]);
    }


}
