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
package hivemall.model;

import javax.annotation.Nonnegative;

public class WeightValueWithClock implements IWeightValue {

    protected float value;
    protected short clock;
    protected byte deltaUpdates;

    public WeightValueWithClock(float value) {
        this.value = value;
        this.clock = 0;
        this.deltaUpdates = 0;
    }

    public WeightValueWithClock(IWeightValue src) {
        this.value = src.get();
        if (src.isTouched()) {
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
    public void setSumOfSquaredGradients(float value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public float getSumOfSquaredDeltaX() {
        return 0.f;
    }

    @Override
    public void setSumOfSquaredDeltaX(float value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public float getSumOfGradients() {
        return 0.f;
    }

    @Override
    public void setSumOfGradients(float value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public float getM() {
        return 0.f;
    }

    @Override
    public void setM(float value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public float getV() {
        return 0.f;
    }

    @Override
    public void setV(float value) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return whether touched in training or not
     */
    public final boolean isTouched() {
        return deltaUpdates > 0;
    }

    @Override
    public void setTouched(boolean touched) {
        throw new UnsupportedOperationException(
            "WeightValueWithClock#setTouched should not be called");
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
        if (deltaUpdates < 0) {
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
        private float f1;

        public WeightValueParamsF1Clock(float value, float f1) {
            super(value);
            this.f1 = f1;
        }

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
            if (i == 1) {
                return f1;
            }
            throw new IllegalArgumentException("getFloatParams(" + i + ") should not be called");
        }

        @Override
        public float getSumOfSquaredGradients() {
            return f1;
        }

        @Override
        public void setSumOfSquaredGradients(float value) {
            this.f1 = value;
        }

    }

    public static final class WeightValueParamsF2Clock extends WeightValueWithClock {
        private float f1;
        private float f2;

        public WeightValueParamsF2Clock(float value, float f1, float f2) {
            super(value);
            this.f1 = f1;
            this.f2 = f2;
        }

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
            if (i == 1) {
                return f1;
            } else if (i == 2) {
                return f2;
            }
            throw new IllegalArgumentException("getFloatParams(" + i + ") should not be called");
        }

        @Override
        public float getSumOfSquaredGradients() {
            return f1;
        }

        @Override
        public void setSumOfSquaredGradients(float value) {
            this.f1 = value;
        }

        @Override
        public float getSumOfSquaredDeltaX() {
            return f2;
        }

        @Override
        public void setSumOfSquaredDeltaX(float value) {
            this.f2 = value;
        }

        @Override
        public float getSumOfGradients() {
            return f2;
        }

        @Override
        public void setSumOfGradients(float value) {
            this.f2 = value;
        }
        @Override
        public float getM() {
            return f1;
        }

        @Override
        public void setM(float value) {
            this.f1 = value;
        }

        @Override
        public float getV() {
            return f2;
        }

        @Override
        public void setV(float value) {
            this.f2 = value;
        }

    }

    public static final class WeightValueParamsF3Clock extends WeightValueWithClock {
        private float f1;
        private float f2;
        private float f3;

        public WeightValueParamsF3Clock(float value, float f1, float f2, float f3) {
            super(value);
            this.f1 = f1;
            this.f2 = f2;
            this.f3 = f3;
        }

        public WeightValueParamsF3Clock(IWeightValue src) {
            super(src);
            this.f1 = src.getFloatParams(1);
            this.f2 = src.getFloatParams(2);
            this.f3 = src.getFloatParams(3);
        }

        @Override
        public WeightValueType getType() {
            return WeightValueType.ParamsF3;
        }

        @Override
        public float getFloatParams(@Nonnegative final int i) {
            if(i == 1) {
                return f1;
            } else if(i == 2) {
                return f2;
            } else if(i == 3) {
                return f3;
            }
            throw new IllegalArgumentException("getFloatParams(" + i + ") should not be called");
        }

        @Override
        public float getSumOfSquaredGradients() {
            return f1;
        }

        @Override
        public void setSumOfSquaredGradients(float value) {
            this.f1 = value;
        }

        @Override
        public float getSumOfSquaredDeltaX() {
            return f2;
        }

        @Override
        public void setSumOfSquaredDeltaX(float value) {
            this.f2 = value;
        }

        @Override
        public float getSumOfGradients() {
            return f3;
        }

        @Override
        public void setSumOfGradients(float value) {
            this.f3 = value;
        }
        @Override
        public float getM() {
            return f1;
        }

        @Override
        public void setM(float value) {
            this.f1 = value;
        }

        @Override
        public float getV() {
            return f2;
        }

        @Override
        public void setV(float value) {
            this.f2 = value;
        }

    }

    public static final class WeightValueWithCovarClock extends WeightValueWithClock {
        public static final float DEFAULT_COVAR = 1.f;

        private float covariance;

        public WeightValueWithCovarClock(float value, float covar) {
            super(value);
            this.covariance = covar;
        }

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
