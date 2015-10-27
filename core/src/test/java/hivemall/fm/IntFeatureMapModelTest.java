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
package hivemall.fm;

import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Test;

public class IntFeatureMapModelTest {
    private static final boolean DEBUG_PRINT = false;

    private static void println(String msg) {
        if(DEBUG_PRINT)
            System.out.println(msg);
    }

    @Test
    public void testClassification() throws HiveException {
        final int ROW = 10, COL = 40;

        FactorizationMachineUDTF udtf = new FactorizationMachineUDTF();
        ListObjectInspector xOI = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        DoubleObjectInspector yOI = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        ObjectInspector paramOI = ObjectInspectorUtils.getConstantObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector, "-adareg -int_feature -factor 20 -classification -seed 31 -iters 10");
        udtf.initialize(new ObjectInspector[] { xOI, yOI, paramOI });
        FactorizationMachineModel model = udtf.getModel();
        Assert.assertTrue("Actual class: " + model.getClass().getName(), model instanceof FMIntFeatureMapModel);

        float accuracy = 0.f;
        final Random rnd = new Random(201L);
        for(int numberOfIteration = 0; numberOfIteration < 10000; numberOfIteration++) {
            ArrayList<IntFeature[]> fArrayList = new ArrayList<IntFeature[]>();
            ArrayList<Double> ans = new ArrayList<Double>();
            for(int i = 0; i < ROW; i++) {
                ArrayList<IntFeature> feature = new ArrayList<IntFeature>();
                for(int j = 1; j <= COL; j++) {
                    if(i < (0.5f * ROW)) {
                        if(j == 1) {
                            feature.add(new IntFeature(j, 1.d));
                        } else if(j < 0.5 * COL) {
                            if(rnd.nextFloat() < 0.2f) {
                                feature.add(new IntFeature(j, rnd.nextDouble()));
                            }
                        }
                    } else {
                        if(j > 0.5f * COL) {
                            if(rnd.nextFloat() < 0.2f) {
                                feature.add(new IntFeature(j, rnd.nextDouble()));
                            }
                        }
                    }
                }
                IntFeature[] x = new IntFeature[feature.size()];
                feature.toArray(x);
                fArrayList.add(x);

                final double y;
                if(i < ROW * 0.5f) {
                    y = -1.0d;
                } else {
                    y = 1.0d;
                }
                ans.add(y);

                udtf.process(new Object[] { toStringArray(x), y });
            }
            int bingo = 0;
            int total = fArrayList.size();
            for(int i = 0; i < total; i++) {
                double tmpAns = ans.get(i);
                if(tmpAns < 0) {
                    tmpAns = 0;
                } else {
                    tmpAns = 1;
                }
                double p = model.predict(fArrayList.get(i));
                int predicted = p > 0.5 ? 1 : 0;
                if(predicted == tmpAns) {
                    bingo++;
                }
            }
            accuracy = bingo / (float) total;
            println("Accuracy = " + accuracy);
        }
        udtf.runTrainingIteration(10);
        Assert.assertTrue(accuracy > 0.95f);
    }

    @Test
    public void testRegression() throws HiveException {
        final int ROW = 1000, COL = 80;

        FactorizationMachineUDTF udtf = new FactorizationMachineUDTF();
        ListObjectInspector xOI = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        DoubleObjectInspector yOI = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        ObjectInspector paramOI = ObjectInspectorUtils.getConstantObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector, "-int_feature -factor 20 -seed 31 -eta 0.001 -lambda0 0.1 -sigma 0.1");
        udtf.initialize(new ObjectInspector[] { xOI, yOI, paramOI });
        FactorizationMachineModel model = udtf.getModel();
        Assert.assertTrue("Actual class: " + model.getClass().getName(), model instanceof FMIntFeatureMapModel);

        double diff = 0.d;
        final Random rnd = new Random(201L);
        for(int numberOfIteration = 0; numberOfIteration < 100; numberOfIteration++) {
            ArrayList<IntFeature[]> fArrayList = new ArrayList<IntFeature[]>();
            ArrayList<Double> ans = new ArrayList<Double>();
            for(int i = 0; i < ROW; i++) {
                ArrayList<IntFeature> feature = new ArrayList<IntFeature>();
                for(int j = 1; j <= COL; j++) {
                    if(i < (0.5f * ROW)) {
                        if(j == 1) {
                            feature.add(new IntFeature(j, 1.d));
                        } else if(j < 0.5 * COL) {
                            if(rnd.nextFloat() < 0.2f) {
                                feature.add(new IntFeature(j, rnd.nextDouble()));
                            }
                        }
                    } else {
                        if(j > (0.5f * COL)) {
                            if(rnd.nextFloat() < 0.2f) {
                                feature.add(new IntFeature(j, rnd.nextDouble()));
                            }
                        }
                    }
                }
                IntFeature[] x = new IntFeature[feature.size()];
                feature.toArray(x);
                fArrayList.add(x);

                final double y;
                if(i < ROW * 0.5f) {
                    y = 0.1d;
                } else {
                    y = 0.4d;
                }
                ans.add(y);

                udtf.process(new Object[] { toStringArray(x), y });
            }

            diff = 0.d;
            for(int i = 0; i < fArrayList.size(); i++) {
                double predicted = model.predict(fArrayList.get(i));
                double actual = ans.get(i);
                double tmpDiff = predicted - actual;
                diff += tmpDiff * tmpDiff;
            }
            println("diff = " + diff);
        }
        Assert.assertTrue("diff = " + diff, diff < 5.d);
    }

    private static String[] toStringArray(IntFeature[] x) {
        final int size = x.length;
        final String[] ret = new String[size];
        for(int i = 0; i < size; i++) {
            ret[i] = x[i].toString();
        }
        return ret;
    }
}
