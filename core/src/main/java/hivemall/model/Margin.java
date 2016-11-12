/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package hivemall.model;

public final class Margin {

    private final float correctScore;
    private final Object maxIncorrectLabel;
    private final float maxIncorrectScore;

    private float variance;

    public Margin(float correctScore, Object maxIncorrectLabel, float maxIncorrectScore) {
        this.correctScore = correctScore;
        this.maxIncorrectLabel = maxIncorrectLabel;
        this.maxIncorrectScore = maxIncorrectScore;
    }

    public float get() {
        return correctScore - maxIncorrectScore;
    }

    public Margin variance(float var) {
        this.variance = var;
        return this;
    }

    public float getCorrectScore() {
        return correctScore;
    }

    public Object getMaxIncorrectLabel() {
        return maxIncorrectLabel;
    }

    public float getMaxIncorrectScore() {
        return maxIncorrectScore;
    }

    public float getVariance() {
        return variance;
    }

}
