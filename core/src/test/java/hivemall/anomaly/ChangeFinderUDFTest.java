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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
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
import org.junit.Assert;
import org.junit.Test;

public class ChangeFinderUDFTest {
    private static final boolean DEBUG = false;
    private static final boolean MAKE_NEW_RAND = false;
    private static final int MAX_LINES = 5000;
    private static final int MAX_WRITE = 1000000000;//1 billion
    private static final int DIM = 3;

    public String writeRand() throws UDFArgumentException, IOException {
        println("generating data");

        UniformIntegerDistribution params = new UniformIntegerDistribution(0, 10);
        PoissonDistribution event = new PoissonDistribution(100.d);
        NormalDistribution dataGenerator[] = new NormalDistribution[DIM];

        String fileName = null;
        File outFile;
        BufferedWriter output = null;
        BufferedWriter output2 = null;
        if (DEBUG) {
            int runCount = 0;
            fileName = "src/test/resources/hivemall/anomaly/rand_output";
            outFile = new File(fileName + runCount + ".dat");
            while (outFile.exists()) {
                runCount++;
                outFile = new File(fileName + runCount + ".dat");
            }
            fileName = outFile.getName();
            output = new BufferedWriter(new FileWriter(outFile));
            output2 = new BufferedWriter(
                new FileWriter("src/test/resources/hivemall/anomaly/rand_hints.txt"));
        }
        {
            int ln = 1;
            for (int i = 0; i < MAX_WRITE;) {
                int len = event.sample();
                if (DEBUG) {
                    output2.write("length " + len + "-" + (ln + len) + ": ");
                }
                double data[][] = new double[DIM][len];
                for (int j = 0; j < DIM; j++) {
                    int mean = params.sample() * 5;
                    int sd = params.sample() / 10 + 1;
                    dataGenerator[j] = new NormalDistribution(mean, sd);
                    data[j] = dataGenerator[j].sample(len);
                    if (DEBUG) {
                        output2.write("N(" + mean + "," + sd + "), ");
                    }
                    data[j][len / (j + 2) + DIM % (j + 1)] = mean + (j + 4) * sd;
                }
                if (DEBUG) {
                    output2.write("\n");
                }
                for (int j = 0; j < len; j++) {
                    String contents = ln + "\t";
                    for (int k = 0; k < DIM - 1; k++) {
                        contents += data[k][j] + ",";
                    }
                    contents += data[DIM - 1][j] + "\n";
                    if (DEBUG) {
                        output.write(contents);
                    }
                    i += contents.length();
                    ln++;
                }
            }
        }

        if (DEBUG) {
            output.close();
            output2.close();
        }
        return fileName.substring(fileName.lastIndexOf('/', fileName.length() - 1) + 1);
    }

    public void testFile(String input) throws HiveException, IOException {
        if (!(new File(input).exists())) {
            println("File DNE: " + input);
            return;
        }
        boolean tsv = input.endsWith(".tsv");
        boolean csv = input.endsWith(".csv");
        boolean dat = input.endsWith(".dat");

        println("detection test");
        ChangeFinderUDF udf = new ChangeFinderUDF();
        ObjectInspector[] argOIs =
                new ObjectInspector[] {ObjectInspectorFactory.getStandardListObjectInspector(
                    PrimitiveObjectInspectorFactory.javaDoubleObjectInspector)};

        udf.initialize(argOIs);

        Object[] result = null;
        BufferedReader data =
                new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(input)));

        String fileName = null;
        File outFile;
        BufferedWriter output = null;
        if (DEBUG) {
            int runCount = 0;
            if (tsv) {
                fileName = "src/test/resources/hivemall/anomaly/tsv_output";
            }
            if (csv) {
                fileName = "src/test/resources/hivemall/anomaly/csv_output";
            }
            if (dat) {
                fileName = "src/test/resources/hivemall/anomaly/dat_output";
            }
            outFile = new File(fileName + ".dat");
            while (outFile.exists()) {
                runCount++;
                outFile = new File(fileName + runCount + ".dat");
            }
            fileName = outFile.getName();
            output = new BufferedWriter(new FileWriter(outFile));
            output.write("#\taW\taF\t\taT\tcW\tcF\t\tcT\n#\t" + udf.getxRunningWindowSize() + "\t"
                    + udf.getxForgetfulness() + "\t" + udf.getxThreshold() + "\t"
                    + udf.getyRunningWindowSize() + "\t" + udf.getyForgetfulness() + "\t"
                    + udf.getyThreshold() + "\n");
        }

        ArrayList<Integer> anomalies = new ArrayList<Integer>();
        ArrayList<Integer> changepoints = new ArrayList<Integer>();
        for (int lineNumber = 0; lineNumber < MAX_LINES; ++lineNumber) {
            //gather features in current line
            final String line = data.readLine();
            if (line == null) {
                println("EOF reached at line " + lineNumber);
                break;
            }
            List<Double> vector = new ArrayList<Double>();

            if (tsv) {
                //cut string into vector values
                String remaining = line;
                int wordCut = remaining.indexOf('\t');
                if (wordCut == -1) {
                    vector.add(Double.parseDouble(remaining));
                }
                while (wordCut != -1) {
                    vector.add(Double.parseDouble(remaining.substring(0, wordCut)));
                    remaining = remaining.substring(wordCut + 1);
                    wordCut = remaining.indexOf('\t');
                }
            }
            if (csv) {
                if (lineNumber == 0) {
                    continue;
                }
                //cut string into vector values
                String remaining = line;
                int wordCut = remaining.lastIndexOf(',');
                if (wordCut == -1) {
                    vector.add(Double.parseDouble(remaining));
                } else {
                    vector.add(Double.parseDouble(remaining.substring(wordCut + 1)));//only grabs last value: see raw_data.csv
                }
            }
            if (dat) {
                //cut string into vector values
                String remaining = line;
                int wordCut = remaining.indexOf('\t');
                remaining = remaining.substring(wordCut + 1);
                wordCut = remaining.indexOf(',');
                while (wordCut != -1) {
                    vector.add(Double.parseDouble(remaining.substring(0, wordCut)));
                    remaining = remaining.substring(wordCut + 1);
                    wordCut = remaining.indexOf(',');
                }
                vector.add(Double.parseDouble(remaining.substring(wordCut + 1)));
            }
            DeferredObject[] a = new DeferredObject[] {new DeferredJavaObject(vector)};
            result = (Object[]) udf.evaluate(a);
            assert result.length == 4;
            double x = ((DoubleWritable) result[0]).get();
            BooleanWritable resX = (BooleanWritable) result[1];
            //double xB = resX == null ? -1.d : (resX.get() ? 1.d : 0.d); //(1, 0, -1) <-> (true, false, null)
            double xB = resX == null ? Double.NaN : (resX.get() ? 1.d : Double.NaN); //(1, NaN, NaN) <-> (true, false, null); useful in gnuplot for plotting individual anomalies because NaNs are not plotted
            double y = ((DoubleWritable) result[2]).get();
            BooleanWritable resY = (BooleanWritable) result[3];
            //double yB = resY == null ? Double.NaN : (resY.get() ? 1.d : Double.NaN); //(1, 0, -1) <-> (true, false, null)
            double yB = resY == null ? Double.NaN : (resY.get() ? 1.d : Double.NaN); //(1, NaN, NaN) <-> (true, false, null); useful in gnuplot for plotting individual change-points because NaNs are not plotted

            if (DEBUG) {
                output.write(x + " " + xB + " " + y + " " + yB + " "
                        + udf.getxEstimate().getEntry(0) + " " + udf.getxModelCovar().getEntry(0, 0)
                        + " " + udf.getyEstimate() + " " + udf.getyModelVar() + "\n");
            }
            if (xB == 1.d) {
                anomalies.add(lineNumber + 1);
            }
            if (yB == 1.d) {
                changepoints.add(lineNumber + 1);
            }
        }
        println("Detected " + anomalies.size() + " anomalies at lines: " + anomalies);
        println("Detected " + changepoints.size() + " change-points at lines: " + changepoints);
        println("Data is in " + fileName);
        data.close();
        if (DEBUG) {
            output.close();
        }
        udf.close();
    }


    @Test
    public void testDetection() throws HiveException, IOException {
        testFile("cf_test.tsv");
        testFile("raw_data.csv");
        String randName = "rand_output1.dat";
        if (MAKE_NEW_RAND) {
            randName = writeRand();
        }
        testFile(randName);
        return;
    }

    @Test
    public void testExceptions() throws HiveException, IOException {
        println("exception test");
        ChangeFinderUDF udf = new ChangeFinderUDF();
        ConstantObjectInspector paramOI = ObjectInspectorUtils.getConstantObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, "-aWindow 2");
        ConstantObjectInspector param2OI = ObjectInspectorUtils.getConstantObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, "-aWindow 1");
        ConstantObjectInspector param3OI = ObjectInspectorUtils.getConstantObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, "-aWindow 2 -aForget 5");
        ConstantObjectInspector param4OI = ObjectInspectorUtils.getConstantObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            "-aWindow 2 -aForget 0.5 -aThresh 0");;
        ObjectInspector[] argOIs =
                new ObjectInspector[] {ObjectInspectorFactory.getStandardListObjectInspector(
                    PrimitiveObjectInspectorFactory.javaDoubleObjectInspector), paramOI};

        boolean caught = false;
        udf.initialize(argOIs);
        try {
            udf.evaluate(
                new DeferredObject[] {new DeferredJavaObject(Arrays.asList(new Double[] {1.d}))});
            udf.evaluate(new DeferredObject[] {
                    new DeferredJavaObject(Arrays.asList(new Double[] {1.d, 1.d}))});
        } catch (HiveException e) {
            caught = true;
        }
        Assert.assertTrue("Dimension check broken.", caught);

        caught = false;
        argOIs[1] = param2OI;
        try {
            udf.initialize(argOIs);
        } catch (UDFArgumentException e) {
            caught = true;
        }
        Assert.assertTrue("Window size check broken.", caught);

        argOIs[1] = param3OI;
        caught = false;
        try {
            udf.initialize(argOIs);
        } catch (UDFArgumentException e) {
            caught = true;
        }
        Assert.assertTrue("Forgetfulness check broken.", caught);

        argOIs[1] = param4OI;
        udf.initialize(argOIs);

        DeferredObject[] one =
                new DeferredObject[] {new DeferredJavaObject(Arrays.asList(new Double[] {1.d}))};
        for (int i = 0; i < udf.getxRunningWindowSize(); i++) {
            udf.evaluate(one);
        }
        Object[] result = (Object[]) udf.evaluate(
            new DeferredObject[] {new DeferredJavaObject(Arrays.asList(new Double[] {10000.d}))});
        Assert.assertTrue("Result length incorrect.", result.length == 4);
        Assert.assertTrue("No anomaly detected for data set [1, 1, ..., 1, 10000].",
            ((BooleanWritable) result[1]).get());
        udf.close();

        println("multidim");
        udf.initialize(argOIs);
        Double[] data = new Double[] {1.d, 1.d, 1.d, 1.d, 1.d};
        DeferredObject[] increasing =
                new DeferredObject[] {new DeferredJavaObject(Arrays.asList(data))};
        for (int i = 0; i < 5; i++) {
            udf.evaluate(increasing);
            for (int j = 0; j < 5; j++) {
                data[j] = new Double(i * i + j);
                increasing[j] = new DeferredJavaObject(Arrays.asList(data));
            }
        }
    }

    private static void println(String line) {
        if (DEBUG) {
            System.out.println(line);
        }
    }

}
