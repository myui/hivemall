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
import hivemall.utils.math.MathUtils;

import java.util.Arrays;
import java.util.Random;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
    protected float _lambdaW0;
    protected float _lambdaW;
    private final float[] _lambdaV;

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
        this._lambdaV = new float[factor];
        Arrays.fill(_lambdaV, lambda0);

        initLearningParams();
    }

    protected void initLearningParams() {}

    public abstract int getSize();

    public abstract int getMinIndex();

    public abstract int getMaxIndex();

    public abstract float getW0();

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
    @Nullable
    public abstract float[] getV(int i);

    /**
     * @param i index value >= 1
     */
    public abstract float getV(int i, int f);

    /**
     * @param i index value >= 1
     */
    protected abstract void setV(int i, int f, float nextVif);

    /**    
     * @param f index value >= 0
     */
    float getLambdaV(int f) {
        return _lambdaV[f];
    }

    final double dloss(@Nonnull final Feature[] x, final double y) {
        double p = predict(x);
        return dloss(p, y);
    }

    final double dloss(double p, final double y) {
        final double ret;
        if(_classification) {
            ret = (MathUtils.sigmoid(p * y) - 1.d) * y;
        } else { // regression            
            p = Math.min(p, _max_target);
            p = Math.max(p, _min_target);
            ret = 2.d * (p - y);
        }
        return ret;
    }

    final double predict(@Nonnull final Feature[] x) {
        // w0
        double ret = getW0();

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
            assert (!Double.isNaN(ret));
        }
        assert (!Double.isNaN(ret));
        return ret;
    }

    final void updateW0(final double dloss, final float eta) {
        float gradW0 = (float) dloss;
        float prevW0 = getW0();
        float nextW0 = prevW0 - eta * (gradW0 + 2.f * _lambdaW0 * prevW0);
        setW(0, nextW0);
    }

    final void updateWi(final double dloss, final int i, final double xi, final float eta) {
        float gradWi = (float) (dloss * xi);
        float wi = getW(i);
        float nextWi = wi - eta * (gradWi + 2.f * _lambdaW * wi);
        setW(i, nextWi);
    }

    @Deprecated
    final void updateV(@Nonnull final Feature[] x, final double dloss, final int i, final int f, final float eta) {
        double h = gradV(x, i, f);
        float gradV = (float) (dloss * h);
        float Vif = getV(i, f);
        float LambdaVf = getLambdaV(f);
        float nextVif = Vif - eta * (gradV + 2.f * LambdaVf * Vif);
        setV(i, f, nextVif);
    }

    final void updateV(final double dloss, final int i, final int f, final double Xi, final double sumViX, final float eta) {
        float Vif = getV(i, f);
        double h = gradV(Xi, Vif, sumViX);
        float gradV = (float) (dloss * h);
        float LambdaVf = getLambdaV(f);
        float nextVif = Vif - eta * (gradV + 2.f * LambdaVf * Vif);
        setV(i, f, nextVif);
    }

    final void updateLambdaW0(final double dloss, final float eta) {
        float lambda_w_grad = -2.f * eta * getW0();
        float lambdaW0 = _lambdaW0 - (float) (eta * dloss * lambda_w_grad);
        this._lambdaW0 = Math.max(0.f, lambdaW0);
    }

    final void updateLambdaW(@Nonnull Feature[] x, double dloss, float eta) {
        double sumWX = 0.d;
        for(Feature e : x) {
            if(e == null) {
                continue;
            }
            int i = e.index;
            double xi = e.value;
            sumWX += getW(i) * xi;
        }
        double lambda_w_grad = -2.f * eta * sumWX;
        float lambdaW = _lambdaW - (float) (eta * dloss * lambda_w_grad);
        this._lambdaW = Math.max(0.f, lambdaW);
    }

    /**
     * <pre>
     * grad_lambdafg   := (grad l(y(x),y)) * -2 * alpha * ((\sum_{j} x_j * v'_jf) * (\sum_{j \in group(g)} x_j * v_jf) - \sum_{j \in group(g)} x^2_j * v_jf * v'_jf)
     *                 := (grad l(y(x),y)) * -2 * alpha * (sum_f_dash * sum_f(g) - sum_f_dash_f(g))
     * sum_f_dash      := \sum_{j} x_j * v'_lj, this is independent of the groups
     * sum_f(g)        := \sum_{j \in group(g)} x_j * v_jf
     * sum_f_dash_f(g) := \sum_{j \in group(g)} x^2_j * v_jf * v'_jf
     *                 := \sum_{j \in group(g)} x_j * v'_jf * x_j * v_jf 
     * v_jf'           := v_jf - alpha ( grad_v_jf + 2 * lambda_v_f * v_jf)
     * </pre>
     */
    final void updateLambdaV(@Nonnull final Feature[] x, final double dloss, final float eta) {
        for(int f = 0, k = _factor; f < k; f++) {
            double sum_f_dash = 0.d, sum_f = 0.d, sum_f_dash_f = 0.d;
            float lambdaVf = getLambdaV(f);

            final double sumVfX = sumVfX(x, f);
            for(Feature e : x) {
                assert (e != null) : Arrays.toString(x);
                int j = e.index;
                double x_j = e.value;

                float v_jf = getV(j, f);
                double gradV = gradV(x_j, v_jf, sumVfX);
                //double gradV = gradV(x, j, f);
                double v_dash = v_jf - eta * (gradV + 2.d * lambdaVf * v_jf);

                sum_f_dash += x_j * v_dash;
                sum_f += x_j * v_jf;
                sum_f_dash_f += x_j * v_dash * x_j * v_jf;
            }

            double lambda_v_grad = -2.f * eta * (sum_f_dash * sum_f - sum_f_dash_f);
            lambdaVf -= eta * dloss * lambda_v_grad;
            _lambdaV[f] = Math.max(0.f, lambdaVf);
        }
    }

    /**
     * <pre>
     * grad_v_if := multi * (x_i * (sum_f - v_if * x_i))
     * sum_f     := \sum_j v_jf * x_j
     * </pre>
     */
    @Deprecated
    private double gradV(@Nonnull final Feature[] x, final int i, final int f) {
        double ret = 0.d;
        double xi = 1.d;
        for(Feature e : x) {
            int j = e.index;
            double xj = e.value;
            if(j == i) {
                xi = xj;
            } else {
                float Vjf = getV(j, f);
                ret += Vjf * xj;
            }
        }
        ret *= xi;
        return ret;
    }

    double[] sumVfX(@Nonnull final Feature[] x) {
        final int k = _factor;
        final double[] ret = new double[k];
        for(int f = 0 ; f < k; f++) {
            ret[f] = sumVfX(x, f);
        }
        return ret;
    }
    
    private double sumVfX(@Nonnull final Feature[] x, final int f) {
        double ret = 0.d;
        for(Feature e : x) {
            int j = e.index;
            double xj = e.value;
            float Vjf = getV(j, f);
            ret += Vjf * xj;
        }
        return ret;
    }

    /**
     * <pre>
     * grad_v_if := multi * (x_i * (sum_f - v_if * x_i))
     * sum_f     := \sum_j v_jf * x_j
     * </pre>
     */
    private double gradV(@Nonnull final double Xj, final float Vjf, final double sumVfX) {
        return Xj * (sumVfX - Vjf * Xj);
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
