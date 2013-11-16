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
package hivemall.utils;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public final class ArrayUtils {

    public static double[] set(double[] src, final int index, final double value) {
        if(index >= src.length) {
            src = Arrays.copyOf(src, src.length * 2);
        }
        src[index] = value;
        return src;
    }

    public static <T> T[] set(T[] src, final int index, final T value) {
        if(index >= src.length) {
            src = Arrays.copyOf(src, src.length * 2);
        }
        src[index] = value;
        return src;
    }

    public static float[] toArray(final List<Float> lst) {
        final int ndim = lst.size();
        final float[] ary = new float[ndim];
        int i = 0;
        for(float f : lst) {
            ary[i++] = f;
        }
        return ary;
    }

    public static Float[] toObject(final float[] array) {
        final Float[] result = new Float[array.length];
        for(int i = 0; i < array.length; i++) {
            result[i] = array[i];
        }
        return result;
    }

    public static List<Float> toList(final float[] array) {
        Float[] v = toObject(array);
        return Arrays.asList(v);
    }

    public static <T> void shuffle(final T[] array) {
        shuffle(array, array.length);
    }

    /**
     * Fisher–Yates shuffle
     * 
     * @link http://en.wikipedia.org/wiki/Fisher%E2%80%93Yates_shuffle
     */
    public static <T> void shuffle(final T[] array, final int size) {
        final Random rnd = new Random();
        for(int i = 0; i < size; i++) {
            int randomPosition = rnd.nextInt(size);
            T temp = array[i];
            array[i] = array[randomPosition];
            array[randomPosition] = temp;
        }
    }
}
