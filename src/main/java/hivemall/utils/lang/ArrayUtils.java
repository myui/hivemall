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

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public final class ArrayUtils {

    private ArrayUtils() {}

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

    public static Integer[] toObject(final int[] array) {
        final Integer[] result = new Integer[array.length];
        for(int i = 0; i < array.length; i++) {
            result[i] = array[i];
        }
        return result;
    }

    public static List<Integer> toList(final int[] array) {
        Integer[] v = toObject(array);
        return Arrays.asList(v);
    }

    public static Long[] toObject(final long[] array) {
        final Long[] result = new Long[array.length];
        for(int i = 0; i < array.length; i++) {
            result[i] = array[i];
        }
        return result;
    }

    public static List<Long> toList(final long[] array) {
        Long[] v = toObject(array);
        return Arrays.asList(v);
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

    public static Double[] toObject(final double[] array) {
        final Double[] result = new Double[array.length];
        for(int i = 0; i < array.length; i++) {
            result[i] = array[i];
        }
        return result;
    }

    public static List<Double> toList(final double[] array) {
        Double[] v = toObject(array);
        return Arrays.asList(v);
    }

    public static <T> void shuffle(final T[] array) {
        shuffle(array, array.length);
    }

    public static <T> void shuffle(final T[] array, final Random rnd) {
        shuffle(array, array.length, rnd);
    }

    public static <T> void shuffle(final T[] array, final int size) {
        Random rnd = new Random();
        shuffle(array, size, rnd);
    }

    /**
     * Fisher–Yates shuffle
     * 
     * @link http://en.wikipedia.org/wiki/Fisher%E2%80%93Yates_shuffle
     */
    public static <T> void shuffle(final T[] array, final int size, final Random rnd) {
        for(int i = size; i > 1; i--) {
            int randomPosition = rnd.nextInt(i);
            swap(array, i - 1, randomPosition);
        }
    }

    public static void swap(final Object[] arr, final int i, final int j) {
        Object tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }

    public static Object[] subarray(Object[] array, int startIndexInclusive, int endIndexExclusive) {
        if(array == null) {
            return null;
        }
        if(startIndexInclusive < 0) {
            startIndexInclusive = 0;
        }
        if(endIndexExclusive > array.length) {
            endIndexExclusive = array.length;
        }
        int newSize = endIndexExclusive - startIndexInclusive;
        Class<?> type = array.getClass().getComponentType();
        if(newSize <= 0) {
            return (Object[]) Array.newInstance(type, 0);
        }
        Object[] subarray = (Object[]) Array.newInstance(type, newSize);
        System.arraycopy(array, startIndexInclusive, subarray, 0, newSize);
        return subarray;
    }

    public static void fill(final float[] a, final Random rand) {
        for(int i = 0, len = a.length; i < len; i++) {
            a[i] = rand.nextFloat();
        }
    }

}
