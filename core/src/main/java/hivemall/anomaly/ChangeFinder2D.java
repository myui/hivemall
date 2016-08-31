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
import hivemall.utils.collections.RingBuffer;
import hivemall.utils.hadoop.HiveUtils;

import javax.annotation.Nonnull;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
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

    private transient double[] _x = null;
    private transient ArrayRealVector _xVector = null;

    ChangeFinder2D(@Nonnull Parameters params, @Nonnull ListObjectInspector listOI)
            throws UDFArgumentTypeException {
        this.listOI = listOI;
        this.elemOI = HiveUtils.asDoubleCompatibleOI(listOI.getListElementObjectInspector());
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
        int k = xRing.size();
        xRing.add(x).toArray(xSeries, false /* LIFO */);
        RealVector x_hat = sdar1.update(xSeries, k);
        double scoreX = sdar1.logLoss(x, x_hat);
        // smoothing
        double y = ChangeFinderUDF.smoothing(outlierScores.add(scoreX));

        // [Stage#2] Change-point Detection
        k = yRing.size();
        yRing.add(y).toArray(ySeries, false /* LIFO */);
        double y_hat = sdar2.update(ySeries, k);
        double lossY = sdar2.logLoss(y, y_hat);
        double scoreY = ChangeFinderUDF.smoothing(changepointScores.add(lossY));

        outScores[0] = scoreX;
        outScores[1] = scoreY;
    }

    @Nonnull
    private ArrayRealVector parseX(final Object arg) throws HiveException {
        if (_x == null) {
            this._x = HiveUtils.asDoubleArray(arg, listOI, elemOI);
            if (_x.length == 0) {
                throw new HiveException("Dimension of x SHOULD be more than zero");
            }
            this._xVector = new ArrayRealVector(_x);
        } else {
            HiveUtils.toDoubleArray(arg, listOI, elemOI, _x);
        }
        return _xVector;
    }

}
