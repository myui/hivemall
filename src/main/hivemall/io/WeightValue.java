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

public class WeightValue implements IWeightValue {

    protected float value;
    protected boolean touched;

    public WeightValue() {}

    public WeightValue(float weight) {
        this(weight, true);
    }

    public WeightValue(float weight, boolean touched) {
        this.value = weight;
        this.touched = touched;
    }

    @Override
    public final float get() {
        return value;
    }

    @Override
    public final void set(float weight) {
        this.value = weight;
    }

    @Override
    public boolean hasCovariance() {
        return false;
    }

    @Override
    public float getCovariance() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCovariance(float cov) {
        throw new UnsupportedOperationException();
    }

    /** 
     * @return whether touched in training or not
     */
    @Override
    public final boolean isTouched() {
        return touched;
    }

    @Override
    public final void setTouched(boolean touched) {
        this.touched = touched;
    }

    @Override
    public final short getClock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void setClock(short clock) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final byte getDeltaUpdates() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void setDeltaUpdates(byte deltaUpdates) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void copyTo(IWeightValue another) {
        another.set(value);
        another.setTouched(touched);
    }

    @Override
    public void copyFrom(IWeightValue another) {
        this.value = another.get();
        this.touched = another.isTouched();
    }

    @Override
    public String toString() {
        return "WeightValue [value=" + value + "]";
    }

    public static final class WeightValueWithCovar extends WeightValue {
        public static final float DEFAULT_COVAR = 1.f;

        private float covariance;

        public WeightValueWithCovar() {
            super();
        }

        public WeightValueWithCovar(float weight, float covariance) {
            this(weight, covariance, true);
        }

        public WeightValueWithCovar(float weight, float covariance, boolean touched) {
            super(weight, touched);
            this.covariance = covariance;
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
            return "WeightValueWithCovar [value=" + value + ", covariance=" + covariance + "]";
        }
    }

}
