/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013
 *   National Institute of Advanced Industrial Science and Technology (AIST)
 *   Registration Number: H25PRO-1520
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package hivemall.io;

import javax.annotation.Nonnegative;

public class WeightValueWithClock implements IWeightValue {

    protected float value;
    protected short clock;
    protected byte deltaUpdates;

    public WeightValueWithClock(IWeightValue src) {
        this.value = src.get();
        if(src.isTouched()) {
            this.clock = 1;
            this.deltaUpdates = 1;
        } else {
            this.clock = 0;
            this.deltaUpdates = 0;
        }
    }

    @Override
    public WeightValueType getType() {
        return WeightValueType.NoParams;
    }

    @Override
    public float getFloatParams(@Nonnegative int i) {
        throw new UnsupportedOperationException("getFloatParams(int) should not be called");
    }

    public final float get() {
        return value;
    }

    public final void set(float weight) {
        this.value = weight;
    }

    public boolean hasCovariance() {
        return false;
    }

    public float getCovariance() {
        throw new UnsupportedOperationException();
    }

    public void setCovariance(float cov) {
        throw new UnsupportedOperationException();
    }

    @Override
    public float getSumOfSquaredGradients() {
        return 0.f;
    }

    @Override
    public float getSumOfSquaredDeltaX() {
        return 0.f;
    }

    @Override
    public float getSumOfGradients() {
        return 0.f;
    }

    /** 
     * @return whether touched in training or not
     */
    public final boolean isTouched() {
        return deltaUpdates > 0;
    }

    @Override
    public void setTouched(boolean touched) {
        throw new UnsupportedOperationException("WeightValueWithClock#setTouched should not be called");
    }

    public final short getClock() {
        return clock;
    }

    public final void setClock(short clock) {
        this.clock = clock;
    }

    public final byte getDeltaUpdates() {
        return deltaUpdates;
    }

    public final void setDeltaUpdates(byte deltaUpdates) {
        if(deltaUpdates < 0) {
            throw new IllegalArgumentException("deltaUpdates is less than 0: " + deltaUpdates);
        }
        this.deltaUpdates = deltaUpdates;
    }

    @Override
    public void copyTo(IWeightValue another) {
        another.set(value);
        another.setClock(clock);
        another.setDeltaUpdates(deltaUpdates);
    }

    @Override
    public void copyFrom(IWeightValue another) {
        this.value = another.get();
        this.clock = another.getClock();
        this.deltaUpdates = another.getDeltaUpdates();
    }

    @Override
    public String toString() {
        return "WeightValueWithClock [value=" + value + ", clock=" + clock + ", deltaUpdates="
                + deltaUpdates + "]";
    }

    /**
     * WeightValue with Sum of Squared Gradients
     */
    public static final class WeightValueParamsF1Clock extends WeightValueWithClock {
        private final float f1;

        public WeightValueParamsF1Clock(IWeightValue src) {
            super(src);
            this.f1 = src.getFloatParams(1);
        }

        @Override
        public WeightValueType getType() {
            return WeightValueType.ParamsF1;
        }

        @Override
        public float getFloatParams(@Nonnegative final int i) {
            if(i == 1) {
                return f1;
            }
            throw new IllegalArgumentException("getFloatParams(" + i + ") should not be called");
        }

        @Override
        public float getSumOfSquaredGradients() {
            return f1;
        }

    }

    public static final class WeightValueParamsF2Clock extends WeightValueWithClock {
        private final float f1;
        private final float f2;

        public WeightValueParamsF2Clock(IWeightValue src) {
            super(src);
            this.f1 = src.getFloatParams(1);
            this.f2 = src.getFloatParams(2);
        }

        @Override
        public WeightValueType getType() {
            return WeightValueType.ParamsF2;
        }

        @Override
        public float getFloatParams(@Nonnegative final int i) {
            if(i == 1) {
                return f1;
            } else if(i == 2) {
                return f2;
            }
            throw new IllegalArgumentException("getFloatParams(" + i + ") should not be called");
        }

        @Override
        public float getSumOfSquaredGradients() {
            return f1;
        }

        @Override
        public float getSumOfSquaredDeltaX() {
            return f2;
        }

        @Override
        public float getSumOfGradients() {
            return f2;
        }

    }

    public static final class WeightValueWithCovarClock extends WeightValueWithClock {
        public static final float DEFAULT_COVAR = 1.f;

        private float covariance;

        public WeightValueWithCovarClock(IWeightValue src) {
            super(src);
            this.covariance = src.getCovariance();
        }

        @Override
        public WeightValueType getType() {
            return WeightValueType.ParamsCovar;
        }

        @Override
        public boolean hasCovariance() {
            return true;
        }

        @Override
        public float getCovariance() {
            return covariance;
        }

        @Override
        public void setCovariance(float cov) {
            this.covariance = cov;
        }

        @Override
        public void copyTo(IWeightValue another) {
            super.copyTo(another);
            another.setCovariance(covariance);
        }

        @Override
        public void copyFrom(IWeightValue another) {
            super.copyFrom(another);
            this.covariance = another.getCovariance();
        }

        @Override
        public String toString() {
            return "WeightValueWithCovar [value=" + value + ", clock=" + clock + ", deltaUpdates="
                    + deltaUpdates + ", covariance=" + covariance + "]";
        }

    }

}