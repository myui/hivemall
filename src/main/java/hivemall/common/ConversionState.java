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
package hivemall.common;

import hivemall.utils.logging.Logging;

public final class ConversionState extends Logging {

    /** Whether to check conversion */
    protected final boolean conversionCheck;
    /** Threshold to determine convergence */
    protected final double convergenceRate;

    /** being ready to end iteration */
    protected boolean readyToFinishIterations;

    /** The cumulative errors in the training */
    protected double totalErrors;
    /** The cumulative losses in an iteration */
    protected double currLosses, prevLosses;

    public ConversionState() {
        this(true, 0.005d);
    }

    public ConversionState(boolean conversionCheck, double convergenceRate) {
        this.conversionCheck = conversionCheck;
        this.convergenceRate = convergenceRate;
        this.readyToFinishIterations = false;
        this.totalErrors = 0.d;
        this.currLosses = 0.d;
        this.prevLosses = Double.POSITIVE_INFINITY;
    }

    public double getTotalErrors() {
        return totalErrors;
    }

    public double getCumulativeLoss() {
        return currLosses;
    }

    public void incrError(double error) {
        this.totalErrors += error;
    }

    public void incrLoss(double loss) {
        this.currLosses += loss;
    }

    public void multiplyLoss(double multi) {
        this.currLosses = currLosses * multi;
    }

    public boolean isConverged(final int iter, final long obserbedTrainingExamples) {
        if(conversionCheck == false) {
            return false;
        }

        if(currLosses > prevLosses) {
            logInfo("currLoss [" + currLosses + "] > prevLosses [" + prevLosses + "]");
            this.prevLosses = currLosses;
            this.currLosses = 0.d;
            this.readyToFinishIterations = false;
            return false;
        }

        final double changeRate = (prevLosses - currLosses) / prevLosses;
        if(changeRate < convergenceRate) {
            if(readyToFinishIterations) {
                // NOTE: never be true at the first iteration where prevLosses == Double.POSITIVE_INFINITY
                logInfo("Training converged at " + iter + "-th iteration. [curLosses="
                        + currLosses + ", prevLosses=" + prevLosses + ", changeRate=" + changeRate
                        + "]");
                return true;
            } else {
                this.readyToFinishIterations = true;
            }
        } else {
            logDebug("iter: " + iter + "[curLosses=" + currLosses + ", prevLosses="
                    + prevLosses + ", changeRate=" + changeRate + ", #trainingExamples="
                    + obserbedTrainingExamples + "]");
            this.readyToFinishIterations = false;
        }

        this.prevLosses = currLosses;
        this.currLosses = 0.d;
        return false;
    }
}
