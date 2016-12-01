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

import hivemall.anomaly.SingularSpectrumTransformUDF.SingularSpectrumTransformInterface;
import hivemall.anomaly.SingularSpectrumTransformUDF.ScoreFunction;
import hivemall.anomaly.SingularSpectrumTransformUDF.Parameters;
import hivemall.utils.collections.DoubleRingBuffer;
import hivemall.utils.math.MatrixUtils;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.SingularValueDecomposition;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

import java.util.Arrays;
import java.util.TreeMap;
import java.util.Collections;

import javax.annotation.Nonnull;

final class SingularSpectrumTransform implements SingularSpectrumTransformInterface {

    @Nonnull
    private final PrimitiveObjectInspector oi;

    @Nonnull
    private final ScoreFunction scoreFunc;

    @Nonnull
    private final int window;
    @Nonnull
    private final int nPastWindow;
    @Nonnull
    private final int nCurrentWindow;
    @Nonnull
    private final int pastSize;
    @Nonnull
    private final int currentSize;
    @Nonnull
    private final int currentOffset;
    @Nonnull
    private final int r;
    @Nonnull
    private final int k;

    @Nonnull
    private final DoubleRingBuffer xRing;
    @Nonnull
    private final double[] xSeries;

    @Nonnull
    private final double[] q;

    SingularSpectrumTransform(@Nonnull Parameters params, @Nonnull PrimitiveObjectInspector oi) {
        this.oi = oi;

        this.scoreFunc = params.scoreFunc;

        this.window = params.w;
        this.nPastWindow = params.n;
        this.nCurrentWindow = params.m;
        this.pastSize = window + nPastWindow;
        this.currentSize = window + nCurrentWindow;
        this.currentOffset = params.g;
        this.r = params.r;
        this.k = params.k;

        // (w + n) past samples for the n-past-windows
        // (w + m) current samples for the m-current-windows, starting from offset g
        // => need to hold past (w + n + g + w + m) samples from the latest sample
        int holdSampleSize = pastSize + currentOffset + currentSize;

        this.xRing = new DoubleRingBuffer(holdSampleSize);
        this.xSeries = new double[holdSampleSize];

        this.q = new double[window];
        double norm = 0.d;
        for (int i = 0; i < window; i++) {
            this.q[i] = Math.random();
            norm += q[i] * q[i];
        }
        norm = Math.sqrt(norm);
        // normalize
        for (int i = 0; i < window; i++) {
            this.q[i] = q[i] / norm;
        }
    }

    @Override
    public void update(@Nonnull final Object arg, @Nonnull final double[] outScores)
            throws HiveException {
        double x = PrimitiveObjectInspectorUtils.getDouble(arg, oi);
        xRing.add(x).toArray(xSeries, true /* FIFO */);

        // need to wait until the buffer is filled
        if (!xRing.isFull()) {
            outScores[0]  = 0.d;
        } else {
            // create past trajectory matrix and find its left singular vectors
            RealMatrix H = new Array2DRowRealMatrix(new double[window][nPastWindow]);
            for (int i = 0; i < nPastWindow; i++) {
                H.setColumn(i, Arrays.copyOfRange(xSeries, i, i + window));
            }

            // create current trajectory matrix and find its left singular vectors
            RealMatrix G = new Array2DRowRealMatrix(new double[window][nCurrentWindow]);
            int currentHead = pastSize + currentOffset;
            for (int i = 0; i < nCurrentWindow; i++) {
                G.setColumn(i, Arrays.copyOfRange(xSeries, currentHead + i, currentHead + i + window));
            }

            switch (scoreFunc) {
                case svd:
                    outScores[0] = computeScoreSVD(H, G);
                    break;
                case ika:
                    outScores[0] = computeScoreIKA(H, G);
                    break;
                default:
                    throw new IllegalStateException("Unexpected score function: " + scoreFunc);
            }
        }
    }

    /**
     * Singular Value Decomposition (SVD) based naive scoring.
     */
    private double computeScoreSVD(@Nonnull final RealMatrix H, @Nonnull final RealMatrix G) {
        SingularValueDecomposition svdH = new SingularValueDecomposition(H);
        RealMatrix UT = svdH.getUT();

        SingularValueDecomposition svdG = new SingularValueDecomposition(G);
        RealMatrix Q = svdG.getU();

        // find the largest singular value for the r principal components
        RealMatrix UTQ = UT.getSubMatrix(0, r - 1, 0, window - 1).multiply(Q.getSubMatrix(0, window - 1, 0, r - 1));
        SingularValueDecomposition svdUTQ = new SingularValueDecomposition(UTQ);
        double[] s = svdUTQ.getSingularValues();

        return 1.d - s[0];
    }

    /**
     * Implicit Krylov Approximation (IKA) based naive scoring.
     *
     * Number of iterations for the Power method and QR method is fixed to 1 for efficiency.
     * This may cause failure (i.e. meaningless scores) depending on datasets and initial values.
     *
     */
    private double computeScoreIKA(@Nonnull final RealMatrix H, @Nonnull final RealMatrix G) {
        // assuming n = m = window, and keep track the left singular vector as `q`
        double firstSingularValue = MatrixUtils.power1(G, q, 1, q, new double[window]);

        RealMatrix T = new Array2DRowRealMatrix(new double[k][k]);
        MatrixUtils.lanczosTridiagonalization(H.multiply(H.transpose()), q, T);

        double[] eigvals = new double[k];
        RealMatrix eigvecs = new Array2DRowRealMatrix(new double[k][k]);
        MatrixUtils.tridiagonalEigen(T, 1, eigvals, eigvecs);

        // tridiagonalEigen() returns unordered eigenvalues,
        // so the top-r eigenvectors should be picked carefully
        TreeMap<Double, Integer> map = new TreeMap<Double, Integer>(Collections.reverseOrder());
        for (int i = 0; i < k; i++) {
            map.put(eigvals[i], i);
        }
        Object[] sortedIndices = map.values().toArray();

        double s = 0.d;
        for (int i = 0; i < r; i++) {
            double v = eigvecs.getEntry(0, (int)sortedIndices[i]);
            s += v * v;
        }
        return 1.d - Math.sqrt(s);
    }
}
