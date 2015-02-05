/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013-2014
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

public class Rating {

    private float weight;

    public Rating(float weight) {
        this.weight = weight;
    }

    public float getWeight() {
        return weight;
    }

    public void setWeight(float weight) {
        this.weight = weight;
    }

    public double getSumOfSquaredGradients() {
        throw new UnsupportedOperationException();
    }

    public void setSumOfSquaredGradients(double sqgrad) {
        throw new UnsupportedOperationException();
    }

    public float getCovariance() {
        throw new UnsupportedOperationException();
    }

    public void setCovariance(float covar) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "Rating [weight=" + weight + "]";
    }

    public static class RatingWithSquaredGrad extends Rating {

        private double sumSquaredGrads;

        public RatingWithSquaredGrad(float weight) {
            this(weight, 0.d);
        }

        public RatingWithSquaredGrad(float weight, double sqgrad) {
            super(weight);
            this.sumSquaredGrads = sqgrad;
        }

        @Override
        public double getSumOfSquaredGradients() {
            return sumSquaredGrads;
        }

        @Override
        public void setSumOfSquaredGradients(double sqgrad) {
            this.sumSquaredGrads = sqgrad;
        }

    }

}
