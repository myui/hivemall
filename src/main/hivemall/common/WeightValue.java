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

    public WeightValue(float weight) {
        this.value = weight;
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

    public static final class WeightValueWithCovar extends WeightValue {

        final float covariance;

        public WeightValueWithCovar(float weight, float covariance) {
            super(weight);
            this.covariance = covariance;
        }

        public float getCovariance() {
            return covariance;
        }
    }

}
