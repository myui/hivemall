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
