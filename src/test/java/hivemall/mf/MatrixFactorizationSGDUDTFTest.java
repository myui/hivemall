/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013-2014
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
package hivemall.mf;

import hivemall.io.FactorizedModel.RankInitScheme;
import hivemall.utils.lang.mutable.MutableInt;

import java.io.File;

import junit.framework.Assert;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.Collector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

public class MatrixFactorizationSGDUDTFTest {
    private static final boolean DEBUG_PRINT = false;

    private static void print(String msg) {
        if(DEBUG_PRINT)
            System.out.print(msg);
    }

    private static void println(String msg) {
        if(DEBUG_PRINT)
            System.out.println(msg);
    }

    private static void println() {
        if(DEBUG_PRINT)
            System.out.println();
    }

    @Test
    public void testDefaultInit() throws HiveException {
        println("--------------------------\n testGaussian()");
        OnlineMatrixFactorizationUDTF mf = new MatrixFactorizationSGDUDTF();

        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector floatOI = PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
        //ObjectInspector param = ObjectInspectorUtils.getConstantObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector, new String("-factor 3 -eta 0.0002"));
        ObjectInspector param = ObjectInspectorUtils.getConstantObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector, new String("-factor 3"));
        ObjectInspector[] argOIs = new ObjectInspector[] { intOI, intOI, floatOI, param };
        mf.initialize(argOIs);
        Assert.assertTrue(mf.rankInit == RankInitScheme.random);

        float[][] rating = { { 5, 3, 0, 1 }, { 4, 0, 0, 1 }, { 1, 1, 0, 5 }, { 1, 0, 0, 4 },
                { 0, 1, 5, 4 } };
        Object[] args = new Object[3];
        final int num_iters = 100;
        for(int iter = 0; iter < num_iters; iter++) {
            for(int row = 0; row < rating.length; row++) {
                for(int col = 0, size = rating[row].length; col < size; col++) {
                    //print(row + "," + col + ",");
                    args[0] = row;
                    args[1] = col;
                    args[2] = (float) rating[row][col];
                    //println((float) rating[row][col]);
                    mf.process(args);
                }
            }
        }
        for(int row = 0; row < rating.length; row++) {
            for(int col = 0, size = rating[row].length; col < size; col++) {
                double predicted = mf.predict(row, col);
                print(rating[row][col] + "[" + predicted + "]\t");
                Assert.assertEquals(rating[row][col], predicted, 0.2d);
            }
            println();
        }
    }

    @Test
    public void testRandInit() throws HiveException {
        println("--------------------------\n testRandInit()");
        OnlineMatrixFactorizationUDTF mf = new MatrixFactorizationSGDUDTF();

        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector floatOI = PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
        ObjectInspector param = ObjectInspectorUtils.getConstantObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector, new String("-factor 3 -rankinit random"));
        ObjectInspector[] argOIs = new ObjectInspector[] { intOI, intOI, floatOI, param };
        mf.initialize(argOIs);
        Assert.assertTrue(mf.rankInit == RankInitScheme.random);

        float[][] rating = { { 5, 3, 0, 1 }, { 4, 0, 0, 1 }, { 1, 1, 0, 5 }, { 1, 0, 0, 4 },
                { 0, 1, 5, 4 } };
        Object[] args = new Object[3];
        final int num_iters = 100;
        for(int iter = 0; iter < num_iters; iter++) {
            for(int row = 0; row < rating.length; row++) {
                for(int col = 0, size = rating[row].length; col < size; col++) {
                    args[0] = row;
                    args[1] = col;
                    args[2] = (float) rating[row][col];
                    mf.process(args);
                }
            }
        }
        for(int row = 0; row < rating.length; row++) {
            for(int col = 0, size = rating[row].length; col < size; col++) {
                double predicted = mf.predict(row, col);
                print(rating[row][col] + "[" + predicted + "]\t");
                Assert.assertEquals(rating[row][col], predicted, 0.2d);
            }
            println();
        }
    }

    @Test
    public void testGaussianInit() throws HiveException {
        println("--------------------------\n testGaussianInit()");
        OnlineMatrixFactorizationUDTF mf = new MatrixFactorizationSGDUDTF();

        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector floatOI = PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
        ObjectInspector param = ObjectInspectorUtils.getConstantObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector, new String("-factor 3 -rankinit gaussian"));
        ObjectInspector[] argOIs = new ObjectInspector[] { intOI, intOI, floatOI, param };
        mf.initialize(argOIs);
        Assert.assertTrue(mf.rankInit == RankInitScheme.gaussian);

        float[][] rating = { { 5, 3, 0, 1 }, { 4, 0, 0, 1 }, { 1, 1, 0, 5 }, { 1, 0, 0, 4 },
                { 0, 1, 5, 4 } };
        Object[] args = new Object[3];
        final int num_iters = 100;
        for(int iter = 0; iter < num_iters; iter++) {
            for(int row = 0; row < rating.length; row++) {
                for(int col = 0, size = rating[row].length; col < size; col++) {
                    args[0] = row;
                    args[1] = col;
                    args[2] = (float) rating[row][col];
                    mf.process(args);
                }
            }
        }
        for(int row = 0; row < rating.length; row++) {
            for(int col = 0, size = rating[row].length; col < size; col++) {
                double predicted = mf.predict(row, col);
                print(rating[row][col] + "[" + predicted + "]\t");
                Assert.assertEquals(rating[row][col], predicted, 0.2d);
            }
            println();
        }
    }

    @Test
    public void testIterationsWithoutFile() throws HiveException {
        println("--------------------------\n testIterationsWithoutFile()");
        OnlineMatrixFactorizationUDTF mf = new MatrixFactorizationSGDUDTF();

        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector floatOI = PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
        int iters = 100;
        ObjectInspector param = ObjectInspectorUtils.getConstantObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector, new String("-factor 3 -iterations "
                + iters));
        ObjectInspector[] argOIs = new ObjectInspector[] { intOI, intOI, floatOI, param };
        mf.initialize(argOIs);
        Assert.assertTrue(mf.rankInit == RankInitScheme.random);

        float[][] rating = { { 5, 3, 0, 1 }, { 4, 0, 0, 1 }, { 1, 1, 0, 5 }, { 1, 0, 0, 4 },
                { 0, 1, 5, 4 } };
        Object[] args = new Object[3];
        for(int row = 0; row < rating.length; row++) {
            for(int col = 0, size = rating[row].length; col < size; col++) {
                args[0] = row;
                args[1] = col;
                args[2] = (float) rating[row][col];
                mf.process(args);
            }
        }
        mf.runIterativeTraining(iters);

        for(int row = 0; row < rating.length; row++) {
            for(int col = 0, size = rating[row].length; col < size; col++) {
                double predicted = mf.predict(row, col);
                print(rating[row][col] + "[" + predicted + "]\t");
                Assert.assertEquals(rating[row][col], predicted, 0.2d);
            }
            println();
        }
    }

    @Test
    public void testIterationsCloseWithoutFile() throws HiveException {
        println("--------------------------\n testIterationsCloseWithoutFile()");
        OnlineMatrixFactorizationUDTF mf = new MatrixFactorizationSGDUDTF();

        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector floatOI = PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
        int iters = 3;
        ObjectInspector param = ObjectInspectorUtils.getConstantObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector, new String("-factor 3 -iterations "
                + iters));
        ObjectInspector[] argOIs = new ObjectInspector[] { intOI, intOI, floatOI, param };
        mf.initialize(argOIs);
        final MutableInt numCollected = new MutableInt(0);
        mf.setCollector(new Collector() {
            @Override
            public void collect(Object input) throws HiveException {
                numCollected.addValue(1);
            }
        });
        Assert.assertTrue(mf.rankInit == RankInitScheme.random);

        float[][] rating = { { 5, 3, 0, 1 }, { 4, 0, 0, 1 }, { 1, 1, 0, 5 }, { 1, 0, 0, 4 },
                { 0, 1, 5, 4 } };
        Object[] args = new Object[3];

        final int num_iters = 100;
        int trainingExamples = 0;
        for(int iter = 0; iter < num_iters; iter++) {
            for(int row = 0; row < rating.length; row++) {
                for(int col = 0, size = rating[row].length; col < size; col++) {
                    args[0] = row;
                    args[1] = col;
                    args[2] = (float) rating[row][col];
                    mf.process(args);
                    trainingExamples++;
                }
            }
        }
        mf.close();
        Assert.assertEquals(trainingExamples * iters, mf.count);
        Assert.assertEquals(5, numCollected.intValue());
    }

    @Test
    public void testIterationsCloseWithFile() throws HiveException {
        println("--------------------------\n testIterationsCloseWithFile()");
        OnlineMatrixFactorizationUDTF mf = new MatrixFactorizationSGDUDTF();

        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector floatOI = PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
        int iters = 3;
        ObjectInspector param = ObjectInspectorUtils.getConstantObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector, new String("-factor 3 -iterations "
                + iters));
        ObjectInspector[] argOIs = new ObjectInspector[] { intOI, intOI, floatOI, param };
        mf.initialize(argOIs);
        final MutableInt numCollected = new MutableInt(0);
        mf.setCollector(new Collector() {
            @Override
            public void collect(Object input) throws HiveException {
                numCollected.addValue(1);
            }
        });
        Assert.assertTrue(mf.rankInit == RankInitScheme.random);

        float[][] rating = { { 5, 3, 0, 1 }, { 4, 0, 0, 1 }, { 1, 1, 0, 5 }, { 1, 0, 0, 4 },
                { 0, 1, 5, 4 } };
        Object[] args = new Object[3];

        final int num_iters = 500;
        int trainingExamples = 0;
        for(int iter = 0; iter < num_iters; iter++) {
            for(int row = 0; row < rating.length; row++) {
                for(int col = 0, size = rating[row].length; col < size; col++) {
                    args[0] = row;
                    args[1] = col;
                    args[2] = (float) rating[row][col];
                    mf.process(args);
                    trainingExamples++;
                }
            }
        }

        File tmpFile = mf.fileIO.getFile();
        mf.close();
        Assert.assertEquals(trainingExamples * iters, mf.count);
        Assert.assertEquals(5, numCollected.intValue());
        Assert.assertFalse(tmpFile.exists());
    }
}
