/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
 * Copyright (C) 2013-2015 National Institute of Advanced Industrial Science and Technology (AIST)
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
package hivemall.fm;

import hivemall.common.EtaEstimator;
import hivemall.fm.FactorizationMachineUDTF.Feature;
import hivemall.utils.math.MathUtils;

import java.util.Arrays;
import java.util.Random;

import javax.annotation.Nonnull;

import org.apache.hadoop.hive.ql.metadata.HiveException;

public abstract class FactorizationMachineModel {

    protected final boolean _classification;
    protected final int _factor;
    protected final double _sigma;
    protected final EtaEstimator _eta;
    protected final Random _rnd;

    // Hyperparameter for regression
    protected final double _min_target;
    protected final double _max_target;

    // Regulation Variables
    protected final float _lambdaW0;
    protected final float _lambdaW;
    protected final float _lambdaV;

    // LEARNING PARAMS
    protected float _w0;

    public FactorizationMachineModel(boolean classification, int factor, float lambda0, double sigma, long seed, double minTarget, double maxTarget, @Nonnull EtaEstimator eta) {
        this._classification = classification;
        this._factor = factor;
        this._sigma = sigma;
        this._eta = eta;
        this._rnd = new Random(seed);

        this._min_target = minTarget;
        this._max_target = maxTarget;

        // Regulation Variables
        this._lambdaW0 = lambda0;
        this._lambdaW = lambda0;
        this._lambdaV = lambda0;

        this._w0 = 0.f;
        initLearningParams();
    }

    protected abstract void initLearningParams();

    public abstract int getSize();

    public abstract float getW(int i);

    public abstract float getV(int i, int f);

    protected abstract void setW(int i, float nextWi);

    protected abstract void setV(int i, int f, float nextVif);

    public void updateW0(@Nonnull Feature[] x, double y, float eta) {
        float grad0 = gLoss0(x, y);
        float nextW0 = _w0 - eta * (grad0 + 2.f * _lambdaW0 * _w0);
        this._w0 = nextW0;
    }

    protected double predict(@Nonnull final Feature[] x) {
        // w0
        double ret = _w0;

        // W
        for(Feature e : x) {
            int j = e.index;
            double xj = e.value;
            ret += getW(j) * xj;
        }

        // V
        for(int f = 0, k = _factor; f < k; f++) {
            double sumVjfXj = 0.d;
            double sumV2X2 = 0.d;

            for(Feature e : x) {
                int j = e.index;
                double xj = e.value;
                float vjf = getV(j, f);
                sumVjfXj += vjf * xj;
                sumV2X2 += (vjf * vjf * xj * xj);
            }
            ret += 0.5d * (sumVjfXj * sumVjfXj - sumV2X2);
            assert (Double.isNaN(ret) == false);
        }
        assert (Double.isNaN(ret) == false);
        return ret;
    }

    public void updateWi(@Nonnull Feature[] x, double y, int i, double xi, float eta) {
        float gradWi = gLossWi(x, y, xi);
        float wi = getW(i);
        float nextWi = wi - eta * (gradWi + 2.f * _lambdaW * wi);
        setW(i, nextWi);
    }

    private float gLoss0(Feature[] x, double y) {
        double ret = -1.d;
        double predict0 = predict(x);
        if(_classification) {
            ret = (MathUtils.sigmoid(predict0 * y) - 1.d) * y;
        } else {// regression
            predict0 = Math.min(predict0, _max_target);
            predict0 = Math.max(predict0, _min_target);
            ret = 2.d * (predict0 - y);
        }
        return (float) ret;
    }

    private float gLossWi(@Nonnull Feature[] x, double y, double xi) {
        double ret = -1.d;
        double predictWi = predict(x);
        if(_classification) {
            ret = (MathUtils.sigmoid(predictWi * y) - 1.d) * y * xi;
        } else {
            double diff = predictWi - y;
            ret = 2.d * diff * xi;
        }
        return (float) ret;
    }

    public void updateV(@Nonnull Feature[] x, double y, int i, int f, float eta) {
        float gradV = gLossV(x, y, i, f);
        float vif = getV(i, f);
        float nextVif = vif - eta * (gradV + 2 * _lambdaV * vif);
        setV(i, f, nextVif);
    }

    private float gLossV(@Nonnull Feature[] x, double y, int i, int f) {
        double ret = -1.d;
        double predictV = predict(x);
        if(!_classification) {
            double diff = predictV - y;
            ret = 2.d * diff * gradV(x, i, f);
        } else {
            ret = (MathUtils.sigmoid(predictV * y) - 1.d) * y * gradV(x, i, f);
        }
        return (float) ret;
    }

    private float gradV(@Nonnull Feature[] x, int i, int f) {
        double ret = 0.d;
        double xi = 1.d;
        for(Feature e : x) {
            int j = e.index;
            double xj = e.value;

            if(j == i) {
                xi = xj; // REVIEWME
            } else {
                ret += getV(i, f) * xj;
            }
        }
        ret *= xi;
        return (float) ret;
    }

    public void check(@Nonnull Feature[] x) throws HiveException {
        for(Feature e : x) {
            if(e != null && e.index < 1) {
                throw new HiveException("Index of x should be greater than or equals to 1: "
                        + Arrays.toString(x));
            }
        }
    }

}
