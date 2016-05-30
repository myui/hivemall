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

import hivemall.fm.FMHyperParameters.FFMHyperParameters;
import hivemall.utils.collections.DoubleArray3D;
import hivemall.utils.collections.IntArrayList;
import hivemall.utils.lang.NumberUtils;

import java.util.Arrays;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public abstract class FieldAwareFactorizationMachineModel extends FactorizationMachineModel {

    @Nonnull
    protected final FFMHyperParameters _params;
    protected final float _eta0_V;
    protected final float _eps;

    protected final boolean _useAdaGrad;
    protected final boolean _useFTRL;

    public FieldAwareFactorizationMachineModel(@Nonnull FFMHyperParameters params) {
        super(params);
        this._params = params;
        this._eta0_V = params.eta0_V;
        this._eps = params.eps;
        this._useAdaGrad = params.useAdaGrad;
        this._useFTRL = params.useFTRL;
    }

    public abstract float getV(@Nonnull Feature x, @Nonnull int yField, int f);

    @Deprecated
    protected abstract void setV(@Nonnull Feature x, @Nonnull int yField, int f, float nextVif);

    @Override
    public float getV(Feature x, int f) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void setV(Feature x, int f, float nextVif) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected final double predict(@Nonnull final Feature[] x) {
        // w0
        double ret = getW0();
        // W
        for (Feature e : x) {
            double xi = e.getValue();
            float wi = getW(e);
            double wx = wi * xi;
            ret += wx;
        }
        // V
        for (int i = 0; i < x.length; i++) {
            final Feature ei = x[i];
            final double xi = ei.getValue();
            final int iField = ei.getField();
            for (int j = i + 1; j < x.length; j++) {
                final Feature ej = x[j];
                final double xj = ej.getValue();
                final int jField = ej.getField();
                for (int f = 0, k = _factor; f < k; f++) {
                    float vijf = getV(ei, jField, f);
                    float vjif = getV(ej, iField, f);
                    ret += vijf * vjif * xi * xj;
                    assert (!Double.isNaN(ret));
                }
            }
        }
        if (!NumberUtils.isFinite(ret)) {
            throw new IllegalStateException("Detected " + ret
                    + " in predict. We recommend to normalize training examples.\n"
                    + "Dumping variables ...\n" + varDump(x));
        }
        return ret;
    }

    void updateV(final double dloss, @Nonnull final Feature x, @Nonnull final int yField,
            final int f, final double sumViX, long t) {
        final double Xi = x.getValue();
        final double h = Xi * sumViX;
        final float gradV = (float) (dloss * h);
        final float lambdaVf = getLambdaV(f);

        final Entry theta = getEntry(x, yField);
        assert (theta.Vf != null) : "theta.Vf is NULL: x=" + x + ", yField=" + yField;
        final float currentV = theta.Vf[f];
        final float eta = etaV(theta, t, gradV);
        final float nextV = currentV - eta * (gradV + 2.f * lambdaVf * currentV);
        if (!NumberUtils.isFinite(nextV)) {
            throw new IllegalStateException("Got " + nextV + " for next V" + f + '['
                    + x.getFeatureIndex() + "]\n" + "Xi=" + Xi + ", Vif=" + currentV + ", h=" + h
                    + ", gradV=" + gradV + ", lambdaVf=" + lambdaVf + ", dloss=" + dloss
                    + ", sumViX=" + sumViX);
        }
        theta.Vf[f] = nextV;
    }

    protected final float etaV(@Nonnull final Entry theta, final long t, final float grad) {
        if (_useAdaGrad) {
            double gg = theta.getSumOfSquaredGradientsV();
            theta.addGradientV(grad);
            return (float) (_eta0_V / Math.sqrt(_eps + gg));
        } else {
            return _eta.eta(t);
        }
    }

    /**
     * sum{XiViaf} where a is field index of Xi
     */
    @Nonnull
    final DoubleArray3D sumVfX(@Nonnull final Feature[] x, @Nonnull final IntArrayList fieldList,
            @Nullable DoubleArray3D cached) {
        final int xSize = x.length;
        final int fieldSize = fieldList.size();
        final int factors = _factor;

        final DoubleArray3D mdarray;
        if (cached == null) {
            mdarray = new DoubleArray3D();
            mdarray.setSanityCheck(false);
        } else {
            mdarray = cached;
        }
        mdarray.configure(xSize, fieldSize, factors);

        for (int i = 0; i < xSize; i++) {
            for (int fieldIndex = 0; fieldIndex < fieldSize; fieldIndex++) {
                final int yField = fieldList.get(fieldIndex);
                for (int f = 0; f < factors; f++) {
                    double val = sumVfX(x, i, yField, f);
                    mdarray.set(i, fieldIndex, f, val);
                }
            }
        }

        return mdarray;
    }

    private double sumVfX(@Nonnull final Feature[] x, final int i, @Nonnull final int yField,
            final int f) {
        final Feature xi = x[i];
        final int xiFeature = xi.getFeatureIndex();
        final double xiValue = xi.getValue();
        final int xiField = xi.getField();
        double ret = 0.d;
        // find all other features whose field matches field
        for (Feature e : x) {
            if (e.getFeatureIndex() == xiFeature) { // ignore x[i] = e
                continue;
            }
            if (e.getField() == yField) { // multiply x_e and v_d,field(e),f
                float Vjf = getV(e, xiField, f);
                ret += Vjf * xiValue;
            }
        }
        if (!NumberUtils.isFinite(ret)) {
            throw new IllegalStateException("Got " + ret + " for sumV[ " + i + "][ " + f + "]X.\n"
                    + "x = " + Arrays.toString(x));
        }
        return ret;
    }

    @Nonnull
    protected abstract Entry getEntry(@Nonnull Feature x);

    @Nonnull
    protected abstract Entry getEntry(@Nonnull Feature x, @Nonnull int yField);

    protected final Entry newEntry(final float W) {
        if (_useFTRL) {
            return new FTRLEntry(W);
        } else if (_useAdaGrad) {
            return new AdaGradEntry(W);
        } else {
            return new Entry(W);
        }
    }

    protected final Entry newEntry(@Nonnull final float[] V) {
        if (_useFTRL) {
            return new FTRLEntry(0.f, V);
        } else if (_useAdaGrad) {
            return new AdaGradEntry(0.f, V);
        } else {
            return new Entry(0.f, V);
        }
    }

    static class Entry {
        float W;
        @Nullable
        float[] Vf;

        Entry(float W) {
            this(W, null);
        }

        Entry(float W, @Nullable float[] Vf) {
            this.W = W;
            this.Vf = Vf;
        }

        double getSumOfSquaredGradientsV() {
            throw new UnsupportedOperationException();
        }

        void addGradientV(float grad) {
            throw new UnsupportedOperationException();
        }

        float updateZ(float gradW, float alpha) {
            throw new UnsupportedOperationException();
        }

        double updateN(float gradW) {
            throw new UnsupportedOperationException();
        }

    }

    static class AdaGradEntry extends Entry {
        /** sum of gradients of V */
        double v_gg;

        AdaGradEntry(float W) {
            this(W, null);
        }

        AdaGradEntry(float W, @Nullable float[] Vf) {
            super(W, Vf);
            this.v_gg = 0.d;
        }

        @Override
        final double getSumOfSquaredGradientsV() {
            return v_gg;
        }

        @Override
        final void addGradientV(final float gradV) {
            this.v_gg += gradV * gradV;
        }

    }

    static final class FTRLEntry extends AdaGradEntry {

        float z;
        /** sum of gradients of W */
        double n;

        FTRLEntry(float W) {
            this(W, null);
        }

        FTRLEntry(float W, @Nullable float[] Vf) {
            super(W, Vf);
            this.z = 0.f;
            this.n = 0.d;
        }

        @Override
        float updateZ(final float gradW, final float alpha) {
            double gg = gradW * gradW;
            float sigma = (float) ((Math.sqrt(n + gg) - Math.sqrt(n)) / alpha);

            float newZ = z + gradW - sigma * W;
            if (!NumberUtils.isFinite(newZ)) {
                throw new IllegalStateException("Got newZ " + newZ + " where z=" + z + ", gradW="
                        + gradW + ", sigma=" + sigma + ", W=" + W + ", n=" + n + ", gg=" + gg
                        + ", alpha=" + alpha);
            }
            this.z = newZ;
            return newZ;
        }

        @Override
        double updateN(final float gradW) {
            double newN = n + gradW * gradW;
            if (!NumberUtils.isFinite(newN)) {
                throw new IllegalStateException("Got newN " + newN + " where n=" + n + ", gradW="
                        + gradW);
            }
            this.n = newN;
            return newN;
        }
    }

}
