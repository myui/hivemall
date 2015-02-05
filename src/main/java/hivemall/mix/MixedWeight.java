/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013-2015
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
package hivemall.mix;

public abstract class MixedWeight {

    private float weight;

    public MixedWeight(float weight) {
        this.weight = weight;
    }

    public float getWeight() {
        return weight;
    }

    public void setWeight(float weight) {
        this.weight = weight;
    }

    public int getDeltaUpdates() {
        return 0; // dummy
    }

    public float getCovar() {
        return 0.f; // dummy
    }

    public void setDeltaUpdates(int deltaUpdates) {
        throw new UnsupportedOperationException();
    }

    public void setCovar(float covar) {
        throw new UnsupportedOperationException();
    }

    public static final class WeightWithDelta extends MixedWeight {

        private byte deltaUpdates;

        public WeightWithDelta(float weight, int deltaUpdates) {
            super(weight);
            this.deltaUpdates = (byte) deltaUpdates;
        }

        @Override
        public int getDeltaUpdates() {
            return deltaUpdates;
        }

        @Override
        public void setDeltaUpdates(int deltaUpdates) {
            this.deltaUpdates = (byte) deltaUpdates;
        }

    }

    public static final class WeightWithCovar extends MixedWeight {

        private float covar;

        public WeightWithCovar(float weight, float covar) {
            super(weight);
            this.covar = covar;
        }

        @Override
        public float getCovar() {
            return covar;
        }

        @Override
        public void setCovar(float covar) {
            this.covar = covar;
        }

    }

}
