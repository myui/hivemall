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
package hivemall.utils.collections;

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

public class DoubleArray3DTest {

    @Test
    public void test() {
        final int size_i = 50, size_j = 50, size_k = 5;

        final DoubleArray3D mdarray = new DoubleArray3D();
        mdarray.configure(size_i, size_j, size_k);

        final Random rand = new Random(31L);
        final double[][][] data = new double[size_i][size_j][size_j];
        for (int i = 0; i < size_i; i++) {
            for (int j = 0; j < size_j; j++) {
                for (int k = 0; k < size_k; k++) {
                    double v = rand.nextDouble();
                    data[i][j][k] = v;
                    mdarray.set(i, j, k, v);
                }
            }
        }

        Assert.assertEquals(size_i * size_j * size_k, mdarray.getSize());

        for (int i = 0; i < size_i; i++) {
            for (int j = 0; j < size_j; j++) {
                for (int k = 0; k < size_k; k++) {
                    Assert.assertEquals(data[i][j][k], mdarray.get(i, j, k), 0.d);
                }
            }
        }
    }

    @Test
    public void testConfigureExpand() {
        int size_i = 50, size_j = 50, size_k = 5;

        final DoubleArray3D mdarray = new DoubleArray3D();
        mdarray.configure(size_i, size_j, size_k);

        final Random rand = new Random(31L);
        for (int i = 0; i < size_i; i++) {
            for (int j = 0; j < size_j; j++) {
                for (int k = 0; k < size_k; k++) {
                    double v = rand.nextDouble();
                    mdarray.set(i, j, k, v);
                }
            }
        }

        size_i = 101;
        size_j = 101;
        size_k = 11;
        mdarray.configure(size_i, size_j, size_k);
        Assert.assertEquals(size_i * size_j * size_k, mdarray.getCapacity());
        Assert.assertEquals(size_i * size_j * size_k, mdarray.getSize());

        final double[][][] data = new double[size_i][size_j][size_j];
        for (int i = 0; i < size_i; i++) {
            for (int j = 0; j < size_j; j++) {
                for (int k = 0; k < size_k; k++) {
                    double v = rand.nextDouble();
                    data[i][j][k] = v;
                    mdarray.set(i, j, k, v);
                }
            }
        }

        for (int i = 0; i < size_i; i++) {
            for (int j = 0; j < size_j; j++) {
                for (int k = 0; k < size_k; k++) {
                    Assert.assertEquals(data[i][j][k], mdarray.get(i, j, k), 0.d);
                }
            }
        }
    }

    @Test
    public void testConfigureShrink() {
        int size_i = 50, size_j = 50, size_k = 5;

        final DoubleArray3D mdarray = new DoubleArray3D();
        mdarray.configure(size_i, size_j, size_k);

        final Random rand = new Random(31L);
        for (int i = 0; i < size_i; i++) {
            for (int j = 0; j < size_j; j++) {
                for (int k = 0; k < size_k; k++) {
                    double v = rand.nextDouble();
                    mdarray.set(i, j, k, v);
                }
            }
        }

        int capacity = mdarray.getCapacity();
        size_i = 49;
        size_j = 49;
        size_k = 4;
        mdarray.configure(size_i, size_j, size_k);
        Assert.assertEquals(capacity, mdarray.getCapacity());
        Assert.assertEquals(size_i * size_j * size_k, mdarray.getSize());

        final double[][][] data = new double[size_i][size_j][size_j];
        for (int i = 0; i < size_i; i++) {
            for (int j = 0; j < size_j; j++) {
                for (int k = 0; k < size_k; k++) {
                    double v = rand.nextDouble();
                    data[i][j][k] = v;
                    mdarray.set(i, j, k, v);
                }
            }
        }

        for (int i = 0; i < size_i; i++) {
            for (int j = 0; j < size_j; j++) {
                for (int k = 0; k < size_k; k++) {
                    Assert.assertEquals(data[i][j][k], mdarray.get(i, j, k), 0.d);
                }
            }
        }
    }

}
