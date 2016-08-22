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
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
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
    private static final boolean TEST_CSV = false;
    private static final boolean TEST_TSV = false;
    private static final boolean TEST_MULTIDIM = false;
    private static final boolean MAKE_NEW_RAND_MULTIDIM = false;
    private static final int MAX_LINES = 5000;
    private static final int MAX_WRITE = 1000000;//1 million
    private static final int DIM = 5;

    public File writeRand() throws UDFArgumentException, IOException {
        println("generating data");

        UniformIntegerDistribution params = new UniformIntegerDistribution(0, 10);
        PoissonDistribution event = new PoissonDistribution(1000.d);
        NormalDistribution dataGenerator[] = new NormalDistribution[DIM];

        String fileName = null;//Suppresses uninitialized error when DEBUG = false
        File outputFile = null;//Suppresses uninitialized error when DEBUG = false
        File outputHintFile, outputHintCondensedFile;
        BufferedWriter output = null;
        BufferedWriter output2 = null;
        BufferedWriter output3 = null;
        if (DEBUG) {
            outputFile = File.createTempFile("rand_output", ".txt");
            outputFile.deleteOnExit();
            fileName = outputFile.getName();
            outputHintFile =
                    new File("src/test/resources/hivemall/anomaly/" + fileName + "_hints.txt");
            outputHintCondensedFile = new File(
                "src/test/resources/hivemall/anomaly/" + fileName + "_hints_condensed.txt");
            output = new BufferedWriter(new FileWriter(outputFile));
            output2 = new BufferedWriter(new FileWriter(outputHintFile));
            output3 = new BufferedWriter(new FileWriter(outputHintCondensedFile));
        }
        {
            int ln = 1;
            for (int i = 0; i < MAX_WRITE;) {
                int len = event.sample();
                write(output3, "#length " + len + "\t" + (ln + len) + ":\t");
                double data[][] = new double[DIM][len];
                int mean[] = new int[DIM];
                int sd[] = new int[DIM];
                for (int j = 0; j < DIM; j++) {
                    mean[j] = params.sample() * 5;
                    sd[j] = params.sample() / 10 * 5 + 1;
                    if (i % 5 == 0) {
                        mean[j] += 50;
                    }
                    dataGenerator[j] = new NormalDistribution(mean[j], sd[j]);
                    data[j] = dataGenerator[j].sample(len);
                    data[j][len / (j + 2) + DIM % (j + 1)] = mean[j] + (j + 4) * sd[j];
                }
                for (int j = 0; j < len; j++) {
                    for (int k = 0; k < DIM; k++) {
                        write(output2, mean[k] + " " + sd[k] + " ");
                    }
                    write(output2, "\n");
                }
                for (int k = 0; k < DIM; k++) {
                    write(output3, mean[k] + "\t" + sd[k] + "\t");
                }
                write(output3, "\n");
                for (int j = 0; j < len; j++) {
                    String contents = "";
                    for (int k = 0; k < DIM - 1; k++) {
                        contents += data[k][j] + " ";
                    }
                    contents += data[DIM - 1][j] + "\n";
                    write(output, contents);
                    i++;
                    ln++;
                }
            }
        }

        if (DEBUG) {
            output.close();
            output2.close();
            output3.close();
        }
        println(fileName);
        return outputFile;
    }

    public void testFile(InputStream input, String options) throws HiveException, IOException {
        if (input == null) {
            println("File DNE: " + input);
            return;
        }

        println("detection test");
        ChangeFinderUDF udf = new ChangeFinderUDF();
        ConstantObjectInspector paramOI = ObjectInspectorUtils.getConstantObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, options);
        ObjectInspector[] argOIs =
                new ObjectInspector[] {ObjectInspectorFactory.getStandardListObjectInspector(
                    PrimitiveObjectInspectorFactory.javaDoubleObjectInspector), paramOI};

        Object[] result = null;
        BufferedReader data = new BufferedReader(new InputStreamReader(input));
        final String firstLine = data.readLine();
        boolean tsv = firstLine.contains("\t");
        boolean csv = firstLine.contains(",") && !tsv;
        boolean dat = firstLine.contains(" ") && !csv && !tsv;
        boolean single = !(tsv || csv || dat);

        String fileName = null;
        File outFile;
        BufferedWriter output = null;
        if (DEBUG) {
            int runCount = 0;
            /*
             * Add or remove options freely by modifying the empty string inside paramOI
             */
            if (tsv) {
                fileName = "src/test/resources/hivemall/anomaly/tsv_output";
            }
            if (csv) {
                fileName = "src/test/resources/hivemall/anomaly/csv_output";
            }
            if (dat) {
                fileName = "src/test/resources/hivemall/anomaly/dat_output";
            }
            if (single) {
                fileName = "src/test/resources/hivemall/anomaly/1D_output";
            }
            outFile = new File(fileName + ".txt");
            while (outFile.exists()) {
                runCount++;
                outFile = new File(fileName + runCount + ".txt");
            }
            fileName = outFile.getName();
            output = new BufferedWriter(new FileWriter(outFile));
        }

        ArrayList<Integer> anomalies = new ArrayList<Integer>();
        ArrayList<Integer> changepoints = new ArrayList<Integer>();
        udf.initialize(argOIs);
        for (int lineNumber = 1; lineNumber < MAX_LINES; ++lineNumber) {
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
                int wordCut = remaining.indexOf(' ');
                while (wordCut != -1) {
                    vector.add(Double.parseDouble(remaining.substring(0, wordCut)));
                    remaining = remaining.substring(wordCut + 1);
                    wordCut = remaining.indexOf(' ');
                }
                vector.add(Double.parseDouble(remaining.substring(wordCut + 1)));
            }
            if (single) {
                //each line is a 1-word 1D vector
                vector.add(Double.parseDouble(line));
            }
            DeferredObject[] a = new DeferredObject[] {new DeferredJavaObject(vector)};
            result = (Object[]) udf.evaluate(a);
            assert result.length == 4;
            double x = ((DoubleWritable) result[0]).get();
            BooleanWritable resX = (BooleanWritable) result[1];
            double xB = resX == null ? -1.d : (resX.get() ? 1.d : 0.d); //(1, 0, -1) <-> (true, false, null)
            //double xB = resX == null ? Double.NaN : (resX.get() ? 1.d : Double.NaN); //(1, NaN, NaN) <-> (true, false, null); useful in gnuplot for plotting individual anomalies because NaNs are not plotted
            double y = ((DoubleWritable) result[2]).get();
            BooleanWritable resY = (BooleanWritable) result[3];
            double yB = resY == null ? -1.d : (resY.get() ? 1.d : 0.d); //(1, 0, -1) <-> (true, false, null)
            //double yB = resY == null ? Double.NaN : (resY.get() ? 1.d : Double.NaN); //(1, NaN, NaN) <-> (true, false, null); useful in gnuplot for plotting individual change-points because NaNs are not plotted

            if (DEBUG) {
                if (lineNumber == 1) {
                    output.write("#\taW\taF\t\taT\tcW\tcF\t\tcT\n#\t" + udf.getxRunningWindowSize() + "\t"
                            + udf.getxForgetfulness() + "\t" + udf.getxThreshold() + "\t"
                            + udf.getyRunningWindowSize() + "\t" + udf.getyForgetfulness() + "\t"
                            + udf.getyThreshold()
                            + "\n#x\txBool\ty\tyBool\txMu\txHat[0]\txModelCovar[0,0]\tyMu\ty\tyHat\tyModelVar\n");
                }
                output.write(x + " " + xB + " " + y + " " + yB + " ");// + vector.get(0) + " ");
                if (lineNumber >= udf.getxRunningWindowSize()) {
                    output.write(udf.getxMeanEstimate().getEntry(0) + " ");
                }
                if (lineNumber >= udf.getxRunningWindowSize() + 2 * vector.size()) {
                    output.write(udf.getxEstimate().getEntry(0) + " "
                            + udf.getxModelCovar().getEntry(0, 0) + " ");
                }
                if (lineNumber >= udf.getxRunningWindowSize() + 2 * vector.size()
                        + udf.getyRunningWindowSize()) {
                    output.write(udf.getyMeanEstimate() + " ");
                }
                if (lineNumber >= udf.getxRunningWindowSize() + 2 * vector.size()
                        + udf.getyRunningWindowSize() + 2) {
                    output.write(udf.gety() + " " + udf.getyEstimate() + " " + udf.getyModelVar());
                }
                output.write("\n");
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
        if (TEST_TSV) {
            println("cf_test.tsv");
            testFile(getClass().getResourceAsStream("cf_test.tsv"), "-aThresh 20 -cThresh 11 -cForget 0.02 -noFuture");
        } else {
            println("Skipping Sample TSV data (set TEST_TSV to true)");
        }
        if (TEST_CSV) {
            println("raw_data.csv");
            testFile(getClass().getResourceAsStream("raw_data.csv"), "-aThresh 15 -cThresh 15");
        } else {
            println("Skipping Tw CSV data (set TEST_CSV to true)");
        }
        if (TEST_MULTIDIM) {
            println("rand");
            InputStream randFile = getClass().getResourceAsStream("rand_output.txt");//Change filename as needed if not using MAKE_NEW_RAND_MULTIDIM
            if (MAKE_NEW_RAND_MULTIDIM) {
                randFile = new FileInputStream(writeRand());
            }
            testFile(randFile, "-aForget 0.01");
        } else {
            println("Skipping Multidim rand (set TEST_MULTIDIM to true)");
        }
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

        for (int i = 0; i < 10 * udf.getxRunningWindowSize(); i++) {
            DeferredObject[] one = new DeferredObject[] {
                    new DeferredJavaObject(Arrays.asList(new Double[] {new Double((double) i)}))};
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
        Double[] data = new Double[] {1.d, 1.d};
        DeferredObject[] increasing =
                new DeferredObject[] {new DeferredJavaObject(Arrays.asList(data))};
        udf.evaluate(increasing);
        data = new Double[] {3.d, 1.d};
        increasing = new DeferredObject[] {new DeferredJavaObject(Arrays.asList(data))};
        udf.evaluate(increasing);
        data = new Double[] {1.d, 4.d};
        increasing = new DeferredObject[] {new DeferredJavaObject(Arrays.asList(data))};
        udf.evaluate(increasing);
        data = new Double[] {2.d, 2.d};
        increasing = new DeferredObject[] {new DeferredJavaObject(Arrays.asList(data))};
        udf.evaluate(increasing);
        data = new Double[] {1.d, 1.d};
        increasing = new DeferredObject[] {new DeferredJavaObject(Arrays.asList(data))};
        udf.evaluate(increasing);
    }

    private static void write(Writer output2, String content) throws IOException {
        if (DEBUG) {
            output2.write(content);
        }
    }

    private static void println(String line) {
        if (DEBUG) {
            System.out.println(line);
        }
    }

}
