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
