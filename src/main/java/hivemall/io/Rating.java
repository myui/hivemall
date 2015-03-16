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
