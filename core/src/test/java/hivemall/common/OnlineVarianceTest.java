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
package hivemall.common;

import java.util.Collections;
import java.util.ArrayList;
import java.util.Random;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class OnlineVarianceTest {

    @Test
    public void testSimple() {
        OnlineVariance onlineVariance = new OnlineVariance();

        long n = 0L;
        double sum = 0.d;
        double sumOfSquare = 0.d;

        assertEquals(0L, onlineVariance.numSamples());
        assertEquals(0.d, onlineVariance.mean(), 1e-5f);
        assertEquals(0.d, onlineVariance.variance(), 1e-5f);
        assertEquals(0.d, onlineVariance.stddev(), 1e-5f);

        Random rnd = new Random();
        ArrayList<Double> dArrayList = new ArrayList<Double>();

        for (int i = 0; i < 10; i++) {
            double x = rnd.nextDouble();
            dArrayList.add(x);
            onlineVariance.handle(x);

            n++;
            sum += x;
            sumOfSquare += x * x;

            double mean = n > 0 ? (sum / n) : 0.d;
            double sampleVariance = n > 0 ? ((sumOfSquare / n) - mean * mean) : 0.d;
            double unbiasedVariance = n > 1 ? (sampleVariance * n / (n - 1)) : 0.d;
            double stddev = Math.sqrt(unbiasedVariance);

            assertEquals(n, onlineVariance.numSamples());
            assertEquals(mean, onlineVariance.mean(), 1e-5f);
            assertEquals(unbiasedVariance, onlineVariance.variance(), 1e-5f);
            assertEquals(stddev, onlineVariance.stddev(), 1e-5f);
        }

        Collections.shuffle(dArrayList);

        for (Double x : dArrayList) {
            onlineVariance.unhandle(x.doubleValue());

            n--;
            sum -= x;
            sumOfSquare -= x * x;

            double mean = n > 0 ? (sum / n) : 0.d;
            double sampleVariance = n > 0 ? ((sumOfSquare / n) - mean * mean) : 0.d;
            double unbiasedVariance = n > 1 ? (sampleVariance * n / (n - 1)) : 0.d;
            double stddev = Math.sqrt(unbiasedVariance);

            assertEquals(n, onlineVariance.numSamples());
            assertEquals(mean, onlineVariance.mean(), 1e-5f);
            assertEquals(unbiasedVariance, onlineVariance.variance(), 1e-5f);
            assertEquals(stddev, onlineVariance.stddev(), 1e-5f);
        }

    }

}
