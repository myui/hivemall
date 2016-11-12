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

import javax.annotation.Nonnull;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

final class ChangeFinder1D implements ChangeFinder {

    @Nonnull
    private final PrimitiveObjectInspector oi;
    @Nonnull
    private final LossFunction lossFunc1;
    @Nonnull
    private final LossFunction lossFunc2;

    @Nonnull
    private final SDAR1D sdar1, sdar2;
    @Nonnull
    private final DoubleRingBuffer xRing, yRing;
    @Nonnull
    private final double[] xSeries, ySeries;
    @Nonnull
    private final DoubleRingBuffer outlierScores, changepointScores;

    ChangeFinder1D(@Nonnull Parameters params, @Nonnull PrimitiveObjectInspector oi) {
        this.oi = oi;
        this.lossFunc1 = params.lossFunc1;
        this.lossFunc2 = params.lossFunc2;
        int k = params.k;
        this.sdar1 = new SDAR1D(params.r1, k);
        this.sdar2 = new SDAR1D(params.r2, k);
        this.xRing = new DoubleRingBuffer(k + 1);
        this.yRing = new DoubleRingBuffer(k + 1);
        this.xSeries = new double[k + 1];
        this.ySeries = new double[k + 1];
        this.outlierScores = new DoubleRingBuffer(params.T1);
        this.changepointScores = new DoubleRingBuffer(params.T2);
    }

    @Override
    public void update(@Nonnull final Object arg, @Nonnull final double[] outScores)
            throws HiveException {
        double x = PrimitiveObjectInspectorUtils.getDouble(arg, oi);

        // [Stage#1] Outlier Detection
        xRing.add(x).toArray(xSeries, false /* LIFO */);
        int k1 = xRing.size() - 1;
        double x_hat = sdar1.update(xSeries, k1);

        double scoreX = (k1 == 0.d) ? 0.d : loss(sdar1, x, x_hat, lossFunc1);
        // smoothing
        double y = ChangeFinderUDF.smoothing(outlierScores.add(scoreX));

        // [Stage#2] Change-point Detection
        yRing.add(y).toArray(ySeries, false /* LIFO */);
        int k2 = yRing.size() - 1;
        double y_hat = sdar2.update(ySeries, k2);

        // <LogLoss>
        double lossY = (k2 == 0.d) ? 0.d : loss(sdar2, y, y_hat, lossFunc2);
        double scoreY = ChangeFinderUDF.smoothing(changepointScores.add(lossY));

        outScores[0] = scoreX;
        outScores[1] = scoreY;
    }

    private static double loss(@Nonnull final SDAR1D sdar, @Nonnull final double actual,
            @Nonnull final double predicted, @Nonnull final LossFunction lossFunc) {
        final double loss;
        switch (lossFunc) {
            case hellinger:
                double h2d = sdar.hellingerDistance();
                loss = h2d * 100.d;
                break;
            case logloss:
                loss = sdar.logLoss(actual, predicted);
                break;
            default:
                throw new IllegalStateException("Unexpected loss function: " + lossFunc);
        }
        return loss;
    }
}
