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
package hivemall.utils.lang;

import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.junit.Test;

public class HalfFloatTest {

    @Test
    public void testZero() {
        exactCheck(0f);
        assertEquals((short) 0, HalfFloat.floatToHalfFloat(0f));
    }

    @Test
    public void testIntegers() {
        // Integers between 0 and 2048 can be exactly represented
        for(int i = 0; i <= 2048; i++) {
            exactCheck((float) i);
            exactCheck((float) -i);
        }
        // Integers between 2049 and 4096 round to a multiple of 2 (even number)
        for(int i = 2049; i <= 4096; i++) {
            nonExactCheck((float) i, 1f);
            nonExactCheck((float) -i, 1f);
        }
        // Integers between 4097 and 8192 round to a multiple of 4
        for(int i = 4097; i <= 8192; i++) {
            nonExactCheck((float) i, 3f);
            nonExactCheck((float) -i, 3f);
        }
        // Integers between 8193 and 16384 round to a multiple of 8
        for(int i = 8193; i <= 16384; i++) {
            nonExactCheck((float) i, 7f);
            nonExactCheck((float) -i, 7f);
        }
        // Integers between 16385 and 32768 round to a multiple of 16
        for(int i = 16385; i <= 32768; i++) {
            nonExactCheck((float) i, 15f);
            nonExactCheck((float) -i, 15f);
        }
        // Integers between 32769 and 65519 round to a multiple of 32
        for(int i = 32769; i <= 65519; i++) {
            nonExactCheck((float) i, 31f);
            nonExactCheck((float) -i, 31f);
        }
    }

    @Test
    public void testOverflow() {
        // Integers equal to or above 65520 are rounded to "infinity".
        //exactCheck(65520, Float.POSITIVE_INFINITY);
        //exactCheck(-65520, Float.NEGATIVE_INFINITY);

        final Random rand = new Random();
        final int range = Integer.MAX_VALUE - 65521;
        for(int i = 0; i < 10; i++) {
            int r = rand.nextInt(range);
            exactCheck(65521 + r, Float.POSITIVE_INFINITY);
            exactCheck(-(65521 + r), Float.NEGATIVE_INFINITY);
        }
    }

    @Test
    public void testFiniteValues() {
        for(int i = 0x0000; i < 0x7c00; i++) {
            exactCheck((short) i);
        }
        for(int i = 0x8001; i < 0xfc00; i++) {
            exactCheck((short) i);
        }
    }

    @Test
    public void testSpotcheck() {
        final float test[] = { 1, 5.960464E-8f, 3, 1.788139E-7f, 8, 4.768372E-7f, 16, 9.536743E-7f,
                33, 1.966953E-6f, 83, 4.947186E-6f, 167, 9.953976E-6f, 335, 1.996756E-5f, 838,
                4.994869E-5f, 1677, 9.995699E-05f, 2701, 0.000199914f, 4120, 0.0004997253f, 5144,
                0.0009994507f, 6168, 0.001998901f, 7454, 0.004997253f, 8478, 0.009994507f, 9502,
                0.01998901f, 10854, 0.04998779f, 11878, 0.09997559f, 12902, 0.1999512f, 14336,
                0.5f, 15360, 1f, 16384, 2f, 17664, 5f, 18688, 10f, 19712, 20f, 21056, 50f, 22080,
                100f, 23104, 200f, 24528, 500f, 25552, 1000f, 26576, 2000f, 27874, 5000f, 28898,
                10000f, 29922, 20000f, 31258, 49984f, 32769, -5.960464E-08f, 32771, -1.788139E-07f,
                32776, -4.768372E-07f, 32784, -9.536743E-07f, 32801, -1.966953E-06f, 32851,
                -4.947186E-06f, 32935, -9.953976E-06f, 33103, -1.996756E-05f, 33606,
                -4.994869E-05f, 34445, -9.995699E-05f, 35469, -0.000199914f, 36888, -0.0004997253f,
                37912, -0.0009994507f, 38936, -0.001998901f, 40222, -0.004997253f, 41246,
                -0.009994507f, 42270, -0.01998901f, 43622, -0.04998779f, 44646, -0.09997559f,
                45670, -0.1999512f, 47104, -0.5f, 48128, -1f, 49152, -2f, 50432, -5f, 51456, -10f,
                52480, -20f, 53824, -50f, 54848, -100f, 55872, -200f, 57296, -500f, 58320, -1000f,
                59344, -2000f, 60642, -5000f, 61666, -10000f, 62690, -20000f, 64026, -49984f };
        for(int i = 0; i < test.length; i += 2) {
            nonExactCheck((short) test[i], test[i + 1]);
        }
    }

    @Test
    public void testInfinity() {
        exactCheck(Float.POSITIVE_INFINITY);
        exactCheck(Float.NEGATIVE_INFINITY);
    }

    @Test
    public void testNaN() {
        exactCheck(Float.NaN);
    }

    private static void exactCheck(final short f16) {
        float f = HalfFloat.halfFloatToFloat(f16);
        short hf = HalfFloat.floatToHalfFloat(f);
        assertEquals(f16, hf);
    }

    private static void exactCheck(final float f32) {
        exactCheck(f32, f32);
    }

    private static void exactCheck(final float f32, final float expected) {
        short hf = HalfFloat.floatToHalfFloat(f32);
        float f = HalfFloat.halfFloatToFloat(hf);
        assertEquals(expected, f, 0.0);
    }

    private static void nonExactCheck(final float f32, float delta) {
        short hf = HalfFloat.floatToHalfFloat(f32);
        float f = HalfFloat.halfFloatToFloat(hf);
        assertEquals(f32, f, delta);
    }

    private static void nonExactCheck(final short f16, final float f32) {
        float f = HalfFloat.halfFloatToFloat(f16);
        assertEquals("half float: " + f16, f32, f, 1E-6f);
        short hf = HalfFloat.floatToHalfFloat(f);
        assertEquals(f16, hf, 1E-6f);
    }

}
