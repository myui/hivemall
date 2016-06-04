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

import org.apache.hadoop.hive.ql.metadata.HiveException;

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
    protected final double predict(@Nonnull final Feature[] x) throws HiveException {
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
            throw new HiveException("Detected " + ret
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
        final float currentV = theta.getV(f);
        final float eta = etaV(theta, t, gradV);
        final float nextV = currentV - eta * (gradV + 2.f * lambdaVf * currentV);
        if (!NumberUtils.isFinite(nextV)) {
            throw new IllegalStateException("Got " + nextV + " for next V" + f + '['
                    + x.getFeatureIndex() + "]\n" + "Xi=" + Xi + ", Vif=" + currentV + ", h=" + h
                    + ", gradV=" + gradV + ", lambdaVf=" + lambdaVf + ", dloss=" + dloss
                    + ", sumViX=" + sumViX);
        }
        theta.setV(f, nextV);
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

    @Override
    protected final String varDump(@Nonnull final Feature[] x) {
        final StringBuilder buf = new StringBuilder(1024);
        // X[
        for (int i = 0; i < x.length; i++) {
            Feature e = x[i];
            String j = e.getFeature();
            double xj = e.getValue();
            if (i != 0) {
                buf.append(", ");
            }
            buf.append("x[").append(j).append("] = ").append(xj);
        }
        buf.append("\n");
        // W0
        buf.append("W0 = ").append(getW0()).append('\n');
        // Wi
        for (int i = 0; i < x.length; i++) {
            Feature e = x[i];
            String j = e.getFeature();
            float wi = getW(e);
            if (i != 0) {
                buf.append(", ");
            }
            buf.append("W[").append(j).append("] = ").append(wi);
        }
        buf.append('\n');
        // V
        for (int i = 0; i < x.length; i++) {
            final Feature ei = x[i];
            final int iField = ei.getField();
            if (i != 0) {
                buf.append('\n');
            }
            for (int j = i + 1; j < x.length; j++) {
                final Feature ej = x[j];
                final int jField = ej.getField();
                for (int f = 0, k = _factor; f < k; f++) {
                    float vijf = getV(ei, jField, f);
                    float vjif = getV(ej, iField, f);
                    if (!(j == (i + 1) && f == 0)) {
                        buf.append(", ");
                    }
                    buf.append("V[i")
                       .append(i)
                       .append("][j")
                       .append(j)
                       .append("][f")
                       .append(f)
                       .append("] = ")
                       .append(vijf);
                    buf.append(", V[j")
                       .append(j)
                       .append("][i")
                       .append(i)
                       .append("][f")
                       .append(f)
                       .append("] = ")
                       .append(vjif);
                }
            }
        }
        return buf.toString();
    }
}
