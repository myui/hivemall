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

import hivemall.anomaly.ChangeFinderUDF.ChangeFinder;
import hivemall.anomaly.ChangeFinderUDF.Parameters;
import hivemall.utils.collections.DoubleRingBuffer;

import javax.annotation.Nonnull;

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

final class ChangeFinder1D implements ChangeFinder {

    @Nonnull
    private final PrimitiveObjectInspector oi;
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
    public void update(@Nonnull final Object arg, @Nonnull final double[] outScores) {
        double x = PrimitiveObjectInspectorUtils.getDouble(arg, oi);

        // [Stage#1] Outlier Detection
        xRing.add(x).toArray(xSeries, false /* LIFO */);
        double x_hat = sdar1.update(xSeries);
        double scoreX = sdar1.logLoss(x, x_hat);
        // smoothing
        double y = ChangeFinderUDF.smoothing(outlierScores.add(scoreX));

        // [Stage#2] Change-point Detection
        yRing.add(y).toArray(ySeries, false /* LIFO */);
        double y_hat = sdar2.update(ySeries);
        double lossY = sdar2.logLoss(y, y_hat);
        double scoreY = ChangeFinderUDF.smoothing(changepointScores.add(lossY));

        outScores[0] = scoreX;
        outScores[1] = scoreY;
    }
}

