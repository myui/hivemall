/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
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
package hivemall.utils.retry;

import java.util.Random;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Preconditions;

@NotThreadSafe
public final class ExponentialBackOff implements Backoff {
    public static final long DEFAULT_INITIAL_INTERVAL_MILLIS = 500; // 0.5 sec
    public static final int DEFAULT_STOP_GROWING_THRESHOLD_MILLIS = 60000; // 1 minutes
    public static final double DEFAULT_RANDOMIZATION_FACTOR = 0.3d;
    public static final double DEFAULT_MULTIPLIER = 1.5d;

    private final Random random = new Random();

    private long currentIntervalMillis;
    private final long multiplicationThresholdMillis;
    private final double multiplier;
    private final double randomizationFactor;

    public ExponentialBackOff() {
        this(DEFAULT_INITIAL_INTERVAL_MILLIS, DEFAULT_STOP_GROWING_THRESHOLD_MILLIS, DEFAULT_MULTIPLIER, DEFAULT_RANDOMIZATION_FACTOR);
    }

    public ExponentialBackOff(long initialIntervalMillis, long multiplicationThresholdMillis,
            double multiplier, double randomizationFactor) {
        Preconditions.checkArgument(initialIntervalMillis > 0);
        Preconditions.checkArgument(multiplicationThresholdMillis >= initialIntervalMillis);
        Preconditions.checkArgument(multiplier >= 1);
        Preconditions.checkArgument(0 <= randomizationFactor && randomizationFactor < 1);
        this.currentIntervalMillis = initialIntervalMillis;
        this.multiplicationThresholdMillis = multiplicationThresholdMillis;
        this.multiplier = multiplier;
        this.randomizationFactor = randomizationFactor;
    }

    @Override
    public long computeInterval(int attempt) {
        long interval = getRandomizedInterval(randomizationFactor, random.nextDouble(),
            currentIntervalMillis);
        incrementInterval();
        return interval;
    }

    private static long getRandomizedInterval(double randomizationFactor, double random,
            long interval) {
        double delta = randomizationFactor * interval;
        double minInterval = interval - delta;
        double maxInterval = interval + delta;
        long randomValue = (long) (minInterval + (random * (maxInterval - minInterval + 1.d)));
        return randomValue;
    }

    private void incrementInterval() {
        if (currentIntervalMillis < multiplicationThresholdMillis) {
            this.currentIntervalMillis = Math.min(multiplicationThresholdMillis,
                (long) (currentIntervalMillis * multiplier));
        }
    }

}
