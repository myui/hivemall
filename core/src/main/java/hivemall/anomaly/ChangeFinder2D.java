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

import hivemall.anomaly.ChangeFinderUDF.ChangeFinder;
import hivemall.anomaly.ChangeFinderUDF.LossFunction;
import hivemall.anomaly.ChangeFinderUDF.Parameters;
import hivemall.utils.collections.DoubleRingBuffer;
import hivemall.utils.collections.RingBuffer;
import hivemall.utils.hadoop.HiveUtils;

import javax.annotation.Nonnull;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

final class ChangeFinder2D implements ChangeFinder {

    @Nonnull
    private final ListObjectInspector listOI;
    @Nonnull
    private final PrimitiveObjectInspector elemOI;
    @Nonnull
    private final LossFunction lossFunc1;
    @Nonnull
    private final LossFunction lossFunc2;

    @Nonnull
    private final SDAR2D sdar1;
    @Nonnull
    private final SDAR1D sdar2;
    @Nonnull
    private final RingBuffer<ArrayRealVector> xRing;
    @Nonnull
    private final DoubleRingBuffer yRing;
    @Nonnull
    private final ArrayRealVector[] xSeries;
    @Nonnull
    private final double[] ySeries;
    @Nonnull
    private final DoubleRingBuffer outlierScores, changepointScores;

    ChangeFinder2D(@Nonnull Parameters params, @Nonnull ListObjectInspector listOI)
            throws UDFArgumentTypeException {
        this.listOI = listOI;
        this.elemOI = HiveUtils.asDoubleCompatibleOI(listOI.getListElementObjectInspector());
        this.lossFunc1 = params.lossFunc1;
        this.lossFunc2 = params.lossFunc2;
        int k = params.k;
        this.sdar1 = new SDAR2D(params.r1, k);
        this.sdar2 = new SDAR1D(params.r2, k);
        this.xRing = new RingBuffer<ArrayRealVector>(k + 1);
        this.yRing = new DoubleRingBuffer(k + 1);
        this.xSeries = new ArrayRealVector[k + 1];
        this.ySeries = new double[k + 1];
        this.outlierScores = new DoubleRingBuffer(params.T1);
        this.changepointScores = new DoubleRingBuffer(params.T2);
    }

    @Override
    public void update(@Nonnull final Object arg, @Nonnull final double[] outScores)
            throws HiveException {
        ArrayRealVector x = parseX(arg);

        // [Stage#1] Outlier Detection        
        xRing.add(x).toArray(xSeries, false /* LIFO */);
        int k1 = xRing.size() - 1;
        RealVector x_hat = sdar1.update(xSeries, k1);

        double scoreX = (k1 == 0.d) ? 0.d : loss(x, x_hat, lossFunc1);
        // smoothing
        double y = ChangeFinderUDF.smoothing(outlierScores.add(scoreX));

        // [Stage#2] Change-point Detection
        yRing.add(y).toArray(ySeries, false /* LIFO */);
        int k2 = yRing.size() - 1;
        double y_hat = sdar2.update(ySeries, k2);

        double lossY = (k2 == 0.d) ? 0.d : loss(y, y_hat, lossFunc1);
        double scoreY = ChangeFinderUDF.smoothing(changepointScores.add(lossY));

        outScores[0] = scoreX;
        outScores[1] = scoreY;
    }

    private double loss(@Nonnull final ArrayRealVector x, @Nonnull final RealVector x_hat,
            @Nonnull final LossFunction lossFunc) {
        final double loss;
        switch (lossFunc) {
            case hellinger:
                double h2d = sdar1.hellingerDistance();
                loss = h2d * 100.d;
                break;
            case logloss:
                loss = sdar1.logLoss(x, x_hat);
                break;
            default:
                throw new IllegalStateException("Unexpected loss function: " + lossFunc);
        }
        return loss;
    }

    private double loss(@Nonnull final double y, @Nonnull final double y_hat,
            @Nonnull final LossFunction lossFunc) {
        final double loss;
        switch (lossFunc) {
            case hellinger:
                double h2d = sdar2.hellingerDistance();
                loss = h2d * 100.d;
                break;
            case logloss:
                loss = sdar2.logLoss(y, y_hat);
                break;
            default:
                throw new IllegalStateException("Unexpected loss function: " + lossFunc);
        }
        return loss;
    }

    @Nonnull
    private ArrayRealVector parseX(final Object arg) throws UDFArgumentException {
        ArrayRealVector xVec = xRing.head();
        if (xVec == null) {
            double[] data = HiveUtils.asDoubleArray(arg, listOI, elemOI);
            if (data.length == 0) {
                throw new UDFArgumentException("Dimension of x SHOULD be more than zero");
            }
            xVec = new ArrayRealVector(data, false);
        } else {
            double[] ref = xVec.getDataRef();
            HiveUtils.toDoubleArray(arg, listOI, elemOI, ref, 0.d);
        }
        return xVec;
    }

}
