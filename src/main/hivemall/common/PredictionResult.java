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

public class PredictionResult {

    private final Object predictedLabel;
    private final float predictedScore;

    private float squaredNorm;
    private float variance;

    public PredictionResult(float predictedScore) {
        this(null, predictedScore);
    }

    public PredictionResult(Object predictedLabel, float predictedScore) {
        this.predictedLabel = predictedLabel;
        this.predictedScore = predictedScore;
    }

    public PredictionResult squaredNorm(float sqnorm) {
        this.squaredNorm = sqnorm;
        return this;
    }

    public PredictionResult variance(float var) {
        this.variance = var;
        return this;
    }

    public Object getLabel() {
        return predictedLabel;
    }

    public float getScore() {
        return predictedScore;
    }

    public float getSquaredNorm() {
        return squaredNorm;
    }

    public float getVariance() {
        return variance;
    }

}
