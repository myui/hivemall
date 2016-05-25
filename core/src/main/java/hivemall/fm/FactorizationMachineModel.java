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
import hivemall.utils.lang.NumberUtils;
import hivemall.utils.math.MathUtils;

import java.util.Arrays;
import java.util.Random;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.hive.ql.metadata.HiveException;

public abstract class FactorizationMachineModel {

    protected final boolean _classification;
    protected final int _factor;
    protected final double _sigma;
    protected final EtaEstimator _eta;
    protected final VInitScheme _initScheme;
    protected final Random _rnd;

    // Hyperparameter for regression
    protected final double _min_target;
    protected final double _max_target;

    // Regulation Variables
    protected float _lambdaW0;
    protected float _lambdaW;
    protected final float[] _lambdaV;

    public FactorizationMachineModel(@Nonnull FMHyperParameters params) {
        this._classification = params.classification;
        this._factor = params.factor;
        this._sigma = params.sigma;
        this._eta = params.eta;
        this._initScheme = params.vInit;
        this._rnd = new Random(params.seed);

        this._min_target = params.minTarget;
        this._max_target = params.maxTarget;

        // Regulation Variables
        this._lambdaW0 = params.lambdaW0;
        this._lambdaW = params.lambdaW;
        this._lambdaV = new float[params.factor];
        Arrays.fill(_lambdaV, params.lambdaV);

        initLearningParams();
    }

    protected void initLearningParams() {}

    public abstract int getSize();

    protected int getMinIndex() {
        throw new UnsupportedOperationException();
    }

    protected int getMaxIndex() {
        throw new UnsupportedOperationException();
    }

    public abstract float getW0();

    protected abstract void setW0(float nextW0);

    /**
     * @param i index value >= 1
     */
    protected float getW(int i) {
        throw new UnsupportedOperationException();
    }

    public abstract float getW(@Nonnull Feature x);

    protected abstract void setW(@Nonnull Feature x, float nextWi);

    /**
     * @param i index value >= 1
     */
    @Nullable
    protected float[] getV(int i) {
        throw new UnsupportedOperationException();
    }

    public abstract float getV(@Nonnull Feature x, int f);

    protected abstract void setV(@Nonnull Feature x, int f, float nextVif);

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
        if (_classification) {
            ret = (MathUtils.sigmoid(p * y) - 1.d) * y;
        } else { // regression
            p = Math.min(p, _max_target);
            p = Math.max(p, _min_target);
            //ret = 2.d * (p - y);
            ret = p - y;
        }
        return ret;
    }

    double predict(@Nonnull final Feature[] x) {
        // w0
        double ret = getW0();

        // W
        for (Feature e : x) {
            double xj = e.getValue();
            float w = getW(e);
            double wx = w * xj;
            ret += wx;
        }

        // V
        for (int f = 0, k = _factor; f < k; f++) {
            double sumVjfXj = 0.d;
            double sumV2X2 = 0.d;

            for (Feature e : x) {
                double xj = e.getValue();
                float vjf = getV(e, f);
                double vx = vjf * xj;
                sumVjfXj += vx;
                sumV2X2 += (vx * vx);
            }
            ret += 0.5d * (sumVjfXj * sumVjfXj - sumV2X2);
            assert (!Double.isNaN(ret));
        }
        if (!NumberUtils.isFinite(ret)) {
            throw new IllegalStateException("Detected " + ret
                    + " in predict. We recommend to normalize training examples.\n"
                    + "Dumping variables ...\n" + varDump(x));
        }
        return ret;
    }

    protected final String varDump(@Nonnull final Feature[] x) {
        final StringBuilder buf = new StringBuilder(1024);
        for (int i = 0; i < x.length; i++) {
            Feature e = x[i];
            String j = e.getFeature();
            double xj = e.getValue();
            if (i != 0) {
                buf.append(", ");
            }
            buf.append("x[").append(j).append("] => ").append(xj);
        }
        buf.append("\n\n");
        buf.append("W0 => ").append(getW0()).append('\n');
        for (int i = 0; i < x.length; i++) {
            Feature e = x[i];
            String j = e.getFeature();
            float wi = getW(e);
            if (i != 0) {
                buf.append(", ");
            }
            buf.append("W[").append(j).append("] => ").append(wi);
        }
        buf.append("\n\n");
        for (int f = 0, k = _factor; f < k; f++) {
            for (int i = 0; i < x.length; i++) {
                Feature e = x[i];
                String j = e.getFeature();
                float vjf = getV(e, f);
                if (i != 0) {
                    buf.append(", ");
                }
                buf.append('V').append(f).append('[').append(j).append("] => ").append(vjf);
            }
            buf.append('\n');
        }
        return buf.toString();
    }

    final void updateW0(final double dloss, final float eta) {
        float gradW0 = (float) dloss;
        float prevW0 = getW0();
        float nextW0 = prevW0 - eta * (gradW0 + 2.f * _lambdaW0 * prevW0);
        if (!NumberUtils.isFinite(nextW0)) {
            throw new IllegalStateException("Got " + nextW0 + " for next W0\n" + "gradW0=" + gradW0
                    + ", prevW0=" + prevW0 + ", dloss=" + dloss + ", eta=" + eta);
        }
        setW0(nextW0);
    }

    /**
     * @return whether to update V or not
     */
    void updateWi(final double dloss, @Nonnull final Feature x, final float eta) {
        final double Xi = x.getValue();
        float gradWi = (float) (dloss * Xi);
        float wi = getW(x);
        float nextWi = wi - eta * (gradWi + 2.f * _lambdaW * wi);
        if (!NumberUtils.isFinite(nextWi)) {
            throw new IllegalStateException("Got " + nextWi + " for next W[" + x.getFeature()
                    + "]\n" + "Xi=" + Xi + ", gradWi=" + gradWi + ", wi=" + wi + ", dloss=" + dloss
                    + ", eta=" + eta);
        }
        setW(x, nextWi);
    }

    final void updateV(final double dloss, @Nonnull final Feature x, final int f,
            final double sumViX, final float eta) {
        final double Xi = x.getValue();
        float Vif = getV(x, f);
        double h = gradV(Xi, Vif, sumViX);
        float gradV = (float) (dloss * h);
        float LambdaVf = getLambdaV(f);
        float nextVif = Vif - eta * (gradV + 2.f * LambdaVf * Vif);
        if (!NumberUtils.isFinite(nextVif)) {
            throw new IllegalStateException("Got " + nextVif + " for next V" + f + '['
                    + x.getFeature() + "]\n" + "Xi=" + Xi + ", Vif=" + Vif + ", h=" + h
                    + ", gradV=" + gradV + ", lambdaVf=" + LambdaVf + ", dloss=" + dloss
                    + ", sumViX=" + sumViX + ", eta=" + eta);
        }
        setV(x, f, nextVif);
    }

    final void updateLambdaW0(final double dloss, final float eta) {
        float lambda_w_grad = -2.f * eta * getW0();
        float lambdaW0 = _lambdaW0 - (float) (eta * dloss * lambda_w_grad);
        this._lambdaW0 = Math.max(0.f, lambdaW0);
    }

    final void updateLambdaW(@Nonnull Feature[] x, double dloss, float eta) {
        double sumWX = 0.d;
        for (Feature e : x) {
            assert (e != null) : Arrays.toString(x);
            double xi = e.getValue();
            sumWX += getW(e) * xi;
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
        for (int f = 0, k = _factor; f < k; f++) {
            double sum_f_dash = 0.d, sum_f = 0.d, sum_f_dash_f = 0.d;
            float lambdaVf = getLambdaV(f);

            final double sumVfX = sumVfX(x, f);
            for (Feature e : x) {
                assert (e != null) : Arrays.toString(x);
                double x_j = e.getValue();

                float v_jf = getV(e, f);
                double gradV = gradV(x_j, v_jf, sumVfX);
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

    double[] sumVfX(@Nonnull final Feature[] x) {
        final int k = _factor;
        final double[] ret = new double[k];
        for (int f = 0; f < k; f++) {
            ret[f] = sumVfX(x, f);
        }
        return ret;
    }

    private double sumVfX(@Nonnull final Feature[] x, final int f) {
        double ret = 0.d;
        for (Feature e : x) {
            double xj = e.getValue();
            float Vjf = getV(e, f);
            ret += Vjf * xj;
        }
        if (!NumberUtils.isFinite(ret)) {
            throw new IllegalStateException("Got " + ret + " for sumV[ " + f + "]X.\n" + "x = "
                    + Arrays.toString(x));
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

    public void check(@Nonnull Feature[] x) throws HiveException {}

    public enum VInitScheme {
        random /* default */, gaussian;

        @Nonnegative
        float maxInitValue;
        @Nonnegative
        double initStdDev;
        Random[] rand;

        @Nonnull
        public static VInitScheme resolve(@Nullable String opt) {
            if (opt == null) {
                return random;
            } else if ("gaussian".equalsIgnoreCase(opt)) {
                return gaussian;
            } else if ("random".equalsIgnoreCase(opt)) {
                return random;
            }
            return random;
        }

        public void setMaxInitValue(float maxInitValue) {
            this.maxInitValue = maxInitValue;
        }

        public void setInitStdDev(double initStdDev) {
            this.initStdDev = initStdDev;
        }

        public void initRandom(int factor, long seed) {
            int size = (this == random) ? 1 : factor;
            this.rand = new Random[size];
            for (int i = 0; i < size; i++) {
                rand[i] = new Random(seed + i);
            }
        }
    }

    @Nonnull
    protected final float[] initV() {
        final float[] ret = new float[_factor];
        switch (_initScheme) {
            case random:
                uniformFill(ret, _initScheme.rand[0], _initScheme.maxInitValue);
                break;
            case gaussian:
                gaussianFill(ret, _initScheme.rand, _initScheme.initStdDev);
                break;
            default:
                throw new IllegalStateException("Unsupported V initialization scheme: "
                        + _initScheme);
        }
        return ret;
    }

    protected static final void uniformFill(final float[] a, final Random rand,
            final float maxInitValue) {
        final int len = a.length;
        final float basev = maxInitValue / len;
        for (int i = 0; i < len; i++) {
            float v = rand.nextFloat() * basev;
            a[i] = v;
        }
    }

    protected static final void gaussianFill(final float[] a, final Random[] rand,
            final double stddev) {
        for (int i = 0, len = a.length; i < len; i++) {
            float v = (float) MathUtils.gaussian(0.d, stddev, rand[i]);
            a[i] = v;
        }
    }

}
