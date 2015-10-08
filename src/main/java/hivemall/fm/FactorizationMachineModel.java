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

        initLearningParams();
    }

    protected abstract void initLearningParams();

    public abstract int getSize();

    /**
     * @param i index value >= 0
     */
    public abstract float getW(int i);

    /**
     * @param i index value >= 0
     */
    protected abstract void setW(int i, float nextWi);

    /**
     * @param i index value >= 1
     */
    public abstract float getV(int i, int f);

    /**
     * @param i index value >= 1
     */
    protected abstract void setV(int i, int f, float nextVif);

    public final double dloss(@Nonnull final Feature[] x, final double y) {
        double p = predict(x);
        return dloss(p, y);
    }

    public final double dloss(double p, final double y) {
        final double ret;
        if(_classification) {
            ret = (MathUtils.sigmoid(p * y) - 1.d) * y;
        } else { // regression            
            p = Math.min(p, _max_target);
            p = Math.max(p, _min_target);
            //ret = 2.d * (p - y);
            ret = (p - y);
        }
        return ret;
    }

    protected final double predict(@Nonnull final Feature[] x) {
        // w0
        double ret = getW(0);

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
                double vx = vjf * xj;
                sumVjfXj += vx;
                sumV2X2 += (vx * vx);
            }
            ret += 0.5d * (sumVjfXj * sumVjfXj - sumV2X2);
            assert (Double.isNaN(ret) == false);
        }
        assert (Double.isNaN(ret) == false);
        return ret;
    }

    public final void updateW0(@Nonnull Feature[] x, double dlossMultiplier, float eta) {
        float gradW0 = (float) dlossMultiplier;
        float prevW0 = getW(0);
        float nextW0 = prevW0 - eta * (gradW0 + 2.f * _lambdaW0 * prevW0);
        setW(0, nextW0);
    }

    public final void updateWi(@Nonnull Feature[] x, double dlossMultiplier, int i, double xi, float eta) {
        float gradWi = (float) (dlossMultiplier * xi);
        float wi = getW(i);
        float nextWi = wi - eta * (gradWi + 2.f * _lambdaW * wi);
        setW(i, nextWi);
    }

    public final void updateV(@Nonnull Feature[] x, double dlossMultiplier, int i, int f, float eta) {
        float Vif = getV(i, f);
        float gradV = (float) (dlossMultiplier * gradV(x, i, Vif));
        float nextVif = Vif - eta * (gradV + 2.f * _lambdaV * Vif);
        setV(i, f, nextVif);
    }

    private static double gradV(@Nonnull final Feature[] x, final int i, final float Vif) {
        double ret = 0.d;
        double xi = 1.d;
        for(Feature e : x) {
            int j = e.index;
            double xj = e.value;
            if(j == i) {
                xi = xj;
            } else {
                ret += Vif * xj;
            }
        }
        ret *= xi;
        return ret;
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
