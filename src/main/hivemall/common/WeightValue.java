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
package hivemall.common;

public class WeightValue {

    protected final float value;

    /** Is touched in training */
    protected final boolean touched;

    public WeightValue(float weight) {
        this(weight, true);
    }

    public WeightValue(float weight, boolean touched) {
        this.value = weight;
        this.touched = touched;
    }

    public float get() {
        return value;
    }

    public float getValue() {
        return value;
    }

    public float getCovariance() {
        throw new UnsupportedOperationException();
    }

    /** 
     * @return whether touched in training or not
     */
    public boolean isTouched() {
        return touched;
    }

    @Override
    public String toString() {
        return "WeightValue [value=" + value + "]";
    }

    public static final class WeightValueWithCovar extends WeightValue {
        public static float DEFAULT_COVAR = 1.f;

        final float covariance;

        public WeightValueWithCovar(float weight, float covariance) {
            this(weight, covariance, true);
        }

        public WeightValueWithCovar(float weight, float covariance, boolean touched) {
            super(weight, touched);
            this.covariance = covariance;
        }

        public float getCovariance() {
            return covariance;
        }

        @Override
        public String toString() {
            return "WeightValueWithCovar [value=" + value + ", covariance=" + covariance + "]";
        }
    }

}
