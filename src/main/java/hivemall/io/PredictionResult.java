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
