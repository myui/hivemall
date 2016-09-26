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

import hivemall.anomaly.SSTChangePointUDF.SSTChangePointInterface;
import hivemall.anomaly.SSTChangePointUDF.Parameters;
import hivemall.utils.collections.DoubleRingBuffer;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.SingularValueDecomposition;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

import java.util.Arrays;

import javax.annotation.Nonnull;

final class SSTChangePoint implements SSTChangePointInterface {

    @Nonnull
    private final PrimitiveObjectInspector oi;

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
    private final DoubleRingBuffer xRing;
    @Nonnull
    private final double[] xSeries;

    SSTChangePoint(@Nonnull Parameters params, @Nonnull PrimitiveObjectInspector oi) {
        this.oi = oi;

        this.window = params.w;
        this.nPastWindow = params.n;
        this.nCurrentWindow = params.m;
        this.pastSize = window + nPastWindow;
        this.currentSize = window + nCurrentWindow;
        this.currentOffset = params.g;
        this.r = params.r;

        // (w + n) past samples for the n-past-windows
        // (w + m) current samples for the m-current-windows, starting from offset g
        // => need to hold past (w + n + g + w + m) samples from the latest sample
        int holdSampleSize = pastSize + currentOffset + currentSize;

        this.xRing = new DoubleRingBuffer(holdSampleSize);
        this.xSeries = new double[holdSampleSize];
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
            outScores[0] = computeScore();
        }
    }

    private double computeScore() {
        // create past trajectory matrix and find its left singular vectors
        RealMatrix H = MatrixUtils.createRealMatrix(window, nPastWindow);
        for (int i = 0; i < nPastWindow; i++) {
            H.setColumn(i, Arrays.copyOfRange(xSeries, i, i + window));
        }
        SingularValueDecomposition svdH = new SingularValueDecomposition(H);
        RealMatrix UT = svdH.getUT();

        // create current trajectory matrix and find its left singular vectors
        RealMatrix G = MatrixUtils.createRealMatrix(window, nCurrentWindow);
        int currentHead = pastSize + currentOffset;
        for (int i = 0; i < nCurrentWindow; i++) {
            G.setColumn(i, Arrays.copyOfRange(xSeries, currentHead + i, currentHead + i + window));
        }
        SingularValueDecomposition svdG = new SingularValueDecomposition(G);
        RealMatrix Q = svdG.getU();

        // find the largest singular value for the r principal components
        RealMatrix UTQ = UT.getSubMatrix(0, r - 1, 0, window - 1).multiply(Q.getSubMatrix(0, window - 1, 0, r - 1));
        SingularValueDecomposition svdUTQ = new SingularValueDecomposition(UTQ);
        double[] s = svdUTQ.getSingularValues();

        return 1.d - s[0];
    }
}
