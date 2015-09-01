/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
 * Copyright (c) 2010 Haifeng Li
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
package hivemall.smile.vm;

import static org.junit.Assert.assertEquals;
import hivemall.smile.classification.DecisionTree;
import hivemall.smile.regression.RegressionTree;
import hivemall.smile.tools.TreePredictByStackMachineUDF;
import hivemall.utils.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.text.ParseException;
import java.util.ArrayList;

import javax.script.ScriptException;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.junit.Assert;
import org.junit.Test;

import smile.data.AttributeDataset;
import smile.data.parser.ArffParser;
import smile.math.Math;
import smile.validation.CrossValidation;
import smile.validation.LOOCV;
import smile.validation.Validation;

public class StackMachineTest {
    private static final boolean DEBUG = false;

    /**
     * Test of learn method, of class DecisionTree.
     *
     * @throws ScriptException
     * @throws IOException
     * @throws ParseException
     * @throws HiveException
     */
    @Test
    public void testIris() throws IOException, ParseException, HiveException {
        URL url = new URL("http://people.cs.kuleuven.be/~leander.schietgat/datasets/iris.arff");
        InputStream is = new BufferedInputStream(url.openStream());

        ArffParser arffParser = new ArffParser();
        arffParser.setResponseIndex(4);
        AttributeDataset iris = arffParser.parse(is);
        double[][] x = iris.toArray(new double[iris.size()][]);
        int[] y = iris.toArray(new int[iris.size()]);

        int n = x.length;
        LOOCV loocv = new LOOCV(n);
        for(int i = 0; i < n; i++) {
            double[][] trainx = Math.slice(x, loocv.train[i]);
            int[] trainy = Math.slice(y, loocv.train[i]);

            DecisionTree tree = new DecisionTree(iris.attributes(), trainx, trainy, 4);
            assertEquals(tree.predict(x[loocv.test[i]]), evalPredict(tree, x[loocv.test[i]]));
        }
    }

    @Test
    public void testCpu() throws IOException, ParseException, HiveException {
        // can change the datasets in http://storm.cis.fordham.edu/~gweiss/data-mining/weka-data/~
        URL url = new URL("http://storm.cis.fordham.edu/~gweiss/data-mining/weka-data/cpu.arff");
        // URL url = new URL("http://storm.cis.fordham.edu/~gweiss/data-mining/weka-data/cpu.with.vendor.arff");
        InputStream is = new BufferedInputStream(url.openStream());

        ArffParser arffParser = new ArffParser();
        arffParser.setResponseIndex(6);
        AttributeDataset data = arffParser.parse(is);
        double[] datay = data.toArray(new double[data.size()]);
        double[][] datax = data.toArray(new double[data.size()][]);

        int n = datax.length;
        int k = 10;

        CrossValidation cv = new CrossValidation(n, k);
        for(int i = 0; i < k; i++) {
            double[][] trainx = Math.slice(datax, cv.train[i]);
            double[] trainy = Math.slice(datay, cv.train[i]);
            double[][] testx = Math.slice(datax, cv.test[i]);

            RegressionTree tree = new RegressionTree(data.attributes(), trainx, trainy, 20);

            for(int j = 0; j < testx.length; j++) {
                assertEquals(tree.predict(testx[j]), evalPredict(tree, testx[j]), 1.0);
            }
        }
    }

    @Test
    public void testCpu2() throws IOException, ParseException, HiveException {
        URL url = new URL("http://storm.cis.fordham.edu/~gweiss/data-mining/weka-data/cpu.arff");
        InputStream is = new BufferedInputStream(url.openStream());

        ArffParser arffParser = new ArffParser();
        arffParser.setResponseIndex(6);
        AttributeDataset data = arffParser.parse(is);
        double[] datay = data.toArray(new double[data.size()]);
        double[][] datax = data.toArray(new double[data.size()][]);

        int n = datax.length;
        int m = 3 * n / 4;
        int[] index = Math.permutate(n);

        double[][] trainx = new double[m][];
        double[] trainy = new double[m];
        for(int i = 0; i < m; i++) {
            trainx[i] = datax[index[i]];
            trainy[i] = datay[index[i]];
        }

        double[][] testx = new double[n - m][];
        double[] testy = new double[n - m];
        for(int i = m; i < n; i++) {
            testx[i - m] = datax[index[i]];
            testy[i - m] = datay[index[i]];
        }

        RegressionTree tree = new RegressionTree(data.attributes(), trainx, trainy, 20);
        debugPrint(String.format("RMSE = %.4f\n", Validation.test(tree, testx, testy)));

        for(int i = m; i < n; i++) {
            assertEquals(tree.predict(testx[i - m]), evalPredict(tree, testx[i - m]), 1.0);
        }
    }

    @Test
    public void testFindInfinteLoop() throws IOException, ParseException, HiveException,
            VMRuntimeException {
        // Sample of machine code having infinite loop
        ArrayList<String> opScript = new ArrayList<String>();
        opScript.add("push 2.0");
        opScript.add("push 1.0");
        opScript.add("iflt 0");
        opScript.add("push 1");
        opScript.add("call end");
        debugPrint(opScript);
        double[] x = new double[0];
        StackMachine sm = new StackMachine();
        try {
            sm.run(opScript, x);
            Assert.fail("VMRuntimeException is expected");
        } catch (VMRuntimeException ex) {
            assertEquals("There is a infinite loop in the Machine code.", ex.getMessage());
        }
    }

    @Test
    public void testLargeOpcodes() throws IOException, ParseException, HiveException,
            VMRuntimeException {
        URL url = new URL("https://gist.githubusercontent.com/myui/b1a8e588f5750e3b658c/raw/a4074d37400dab2b13a2f43d81f5166188d3461a/vmtest01.txt");
        InputStream is = new BufferedInputStream(url.openStream());
        String opScript = IOUtils.toString(is);

        double[] x = new double[] { 36, 2, 1, 2, 0, 436, 1, 0, 0, 13, 0, 567, 1, 595, 2, 1 };
        StackMachine sm = new StackMachine();
        sm.run(opScript, x);
        Double result = sm.getResult();
        assertEquals(0.d, result.doubleValue(), 0d);
    }

    private static int evalPredict(DecisionTree tree, double[] x) throws HiveException, IOException {
        TreePredictByStackMachineUDF udf = new TreePredictByStackMachineUDF();
        String opScript = tree.predictOpCodegen(StackMachine.SEP);
        debugPrint(opScript);
        IntWritable result = (IntWritable) udf.evaluate(opScript, x, true);
        udf.close();
        return result.get();
    }

    private static double evalPredict(RegressionTree tree, double[] x) throws HiveException,
            IOException {
        TreePredictByStackMachineUDF udf = new TreePredictByStackMachineUDF();
        String opScript = tree.predictOpCodegen(StackMachine.SEP);
        debugPrint(opScript);
        DoubleWritable result = (DoubleWritable) udf.evaluate(opScript, x, false);
        udf.close();
        return result.get();
    }

    private static void debugPrint(Object msg) {
        if(DEBUG) {
            System.out.println(msg);
        }
    }
}
