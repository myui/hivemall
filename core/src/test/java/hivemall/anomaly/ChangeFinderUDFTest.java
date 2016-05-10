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
package hivemall.anomaly;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.junit.Test;

public class ChangeFinderUDFTest {
    private static final boolean DEBUG = true;
    private static final int MAX_LINES = 5000;

    @Test
    public void testDetection() throws HiveException, IOException {
        println("detection test");
        ChangeFinderUDF udf = new ChangeFinderUDF();
        ObjectInspector[] argOIs =
                new ObjectInspector[] {ObjectInspectorFactory.getStandardListObjectInspector(
                    PrimitiveObjectInspectorFactory.javaDoubleObjectInspector)};

        udf.initialize(argOIs);

        Object[] result = null;
        BufferedReader data = new BufferedReader(
            new InputStreamReader(getClass().getResourceAsStream("cf_test.tsv")));

        ArrayList<Integer> anomalies = new ArrayList<Integer>();
        ArrayList<Integer> changepoints = new ArrayList<Integer>();
        for (int lineNumber = 0; lineNumber < MAX_LINES; ++lineNumber) {
            //gather features in current line
            final String input = data.readLine();
            if (input == null) {
                System.out.println("EOF reached at line " + lineNumber);
                break;
            }
            List<Double> vector = new ArrayList<Double>();

            //cut string into vector values
            String remaining = input;
            int wordCut = remaining.indexOf('\t');
            if (wordCut == -1) {
                vector.add(Double.parseDouble(remaining));
            }
            while (wordCut != -1) {
                vector.add(Double.parseDouble(remaining.substring(0, wordCut)));
                remaining = remaining.substring(wordCut + 1);
                wordCut = remaining.indexOf(' ');
            }
            result = (Object[]) udf.evaluate(new DeferredObject[] {new DeferredJavaObject(vector)});
            assert result.length == 4;
            double x = ((DoubleWritable) result[0]).get();
            String xB =
                    result[1] == null ? null : String.valueOf(((BooleanWritable) result[1]).get());
            double y = ((DoubleWritable) result[2]).get();
            String yB =
                    result[1] == null ? null : String.valueOf(((BooleanWritable) result[3]).get());

            println(x + " " + xB + " " + y + " " + yB);
            if (xB != null && xB.equals("true")) {
                anomalies.add(lineNumber);
                println("!!!ANOMALY AT LINE " + lineNumber + "!!!");
            }
            if (yB != null && yB.equals("true")) {
                changepoints.add(lineNumber);
                println("!!!CHANGE-POINT AT LINE " + lineNumber + "!!!");
            }
        }
        println("Detected " + anomalies.size() + " anomalies at lines: " + anomalies);
        println("Detected " + changepoints.size() + " change-points at lines: " + changepoints);
        data.close();
        udf.close();
    }

    @Test
    public void testOptions() throws HiveException, IOException {
        println("option test");
        ChangeFinderUDF udf = new ChangeFinderUDF();
        ConstantObjectInspector paramOI = ObjectInspectorUtils.getConstantObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, "-dim 2");
        ConstantObjectInspector param2OI = ObjectInspectorUtils.getConstantObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, "-dim 1 -aWindow 0");
        ConstantObjectInspector param3OI = ObjectInspectorUtils.getConstantObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            "-dim 1 -aWindow 1 -aForget 5");
        ConstantObjectInspector param4OI = ObjectInspectorUtils.getConstantObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            "-dim 1 -aWindow 1 -aForget 0.5 -aThresh 0");;
        ObjectInspector[] argOIs =
                new ObjectInspector[] {ObjectInspectorFactory.getStandardListObjectInspector(
                    PrimitiveObjectInspectorFactory.javaDoubleObjectInspector), paramOI};

        boolean caught = false;
        try {
            udf.initialize(argOIs);
        } catch (UDFArgumentException e) {
            caught = true;
        }
        assert caught;

        caught = false;
        argOIs[1] = param2OI;
        try {
            udf.initialize(argOIs);
        } catch (UDFArgumentException e) {
            caught = true;
        }
        assert caught;

        argOIs[1] = param3OI;
        caught = false;
        try {
            udf.initialize(argOIs);
        } catch (UDFArgumentException e) {
            caught = true;
        }
        assert caught;

        argOIs[1] = param4OI;
        udf.initialize(argOIs);

        udf.evaluate(
            new DeferredObject[] {new DeferredJavaObject(Arrays.asList(new Double[] {1.d}))});
        udf.evaluate(
            new DeferredObject[] {new DeferredJavaObject(Arrays.asList(new Double[] {1.d}))});
        udf.evaluate(
            new DeferredObject[] {new DeferredJavaObject(Arrays.asList(new Double[] {1.d}))});
        Object[] result = (Object[]) udf.evaluate(
            new DeferredObject[] {new DeferredJavaObject(Arrays.asList(new Double[] {10000.d}))});
        assert result.length == 4;
        assert PrimitiveObjectInspectorFactory.javaBooleanObjectInspector.get(result[1]);
        udf.close();
    }

    private static void println(String line) {
        if (DEBUG) {
            System.out.println(line);
        }
    }

}
