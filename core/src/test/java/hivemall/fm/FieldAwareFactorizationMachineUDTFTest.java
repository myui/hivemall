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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Test;

public class FieldAwareFactorizationMachineUDTFTest {

    private static final boolean DEBUG = true;
    private static final int ITERATIONS = 50;
    private static final int MAX_LINES = 200;

    @Test
    public void testSGD() throws HiveException, IOException {
        runTest("Pure SGD test",
            "-classification -factor 10 -w0 -seed 43 -disable_adagrad -disable_ftrl", 0.60f);
    }

    @Test
    public void testSGDWithFTRL() throws HiveException, IOException {
        runTest("SGD w/ FTRL test", "-classification -factor 10 -w0 -seed 43 -disable_adagrad",
            0.60f);
    }

    @Test
    public void testAdaGradNoCoeff() throws HiveException, IOException {
        runTest("AdaGrad No Coeff test", "-classification -factor 10 -w0 -seed 43 -no_coeff", 0.30f);
    }

    @Test
    public void testAdaGradNoFTRL() throws HiveException, IOException {
        runTest("AdaGrad w/o FTRL test", "-classification -factor 10 -w0 -seed 43 -disable_ftrl",
            0.30f);
    }

    @Test
    public void testAdaGradDefault() throws HiveException, IOException {
        runTest("AdaGrad DEFAULT (adagrad for V + FTRL for W)",
            "-classification -factor 10 -w0 -seed 43", 0.30f);
    }

    private static void runTest(String testName, String testOptions, float lossThreshold)
            throws IOException, HiveException {
        println(testName);

        FieldAwareFactorizationMachineUDTF udtf = new FieldAwareFactorizationMachineUDTF();
        ObjectInspector[] argOIs = new ObjectInspector[] {
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector),
                PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
                ObjectInspectorUtils.getConstantObjectInspector(
                    PrimitiveObjectInspectorFactory.javaStringObjectInspector, testOptions)};

        udtf.initialize(argOIs);
        FieldAwareFactorizationMachineModel model = (FieldAwareFactorizationMachineModel) udtf.getModel();
        Assert.assertTrue("Actual class: " + model.getClass().getName(),
            model instanceof FFMStringFeatureMapModel);

        double loss = 0.d;
        double cumul = 0.d;
        for (int trainingIteration = 1; trainingIteration <= ITERATIONS; ++trainingIteration) {
            BufferedReader data = new BufferedReader(new InputStreamReader(
                FieldAwareFactorizationMachineUDTFTest.class.getResourceAsStream("bigdata.tr.txt")));
            loss = udtf._cvState.getCumulativeLoss();
            int lines = 0;
            for (int lineNumber = 0; lineNumber < MAX_LINES; ++lineNumber, ++lines) {
                //gather features in current line
                final String input = data.readLine();
                if (input == null) {
                    break;
                }
                ArrayList<String> featureStrings = new ArrayList<String>();
                ArrayList<StringFeature> features = new ArrayList<StringFeature>();

                //make StringFeature for each word = data point
                String remaining = input;
                int wordCut = remaining.indexOf(' ');
                while (wordCut != -1) {
                    featureStrings.add(remaining.substring(0, wordCut));
                    remaining = remaining.substring(wordCut + 1);
                    wordCut = remaining.indexOf(' ');
                }
                int end = featureStrings.size();
                double y = Double.parseDouble(featureStrings.get(0));
                if (y == 0) {
                    y = -1;//LibFFM data uses {0, 1}; Hivemall uses {-1, 1}
                }
                for (int wordNumber = 1; wordNumber < end; ++wordNumber) {
                    String entireFeature = featureStrings.get(wordNumber);
                    int featureCut = StringUtils.ordinalIndexOf(entireFeature, ":", 2);
                    String feature = entireFeature.substring(0, featureCut);
                    double value = Double.parseDouble(entireFeature.substring(featureCut + 1));
                    features.add(new StringFeature(feature, value));
                }
                udtf.process(new Object[] {toStringArray(features), y});
            }
            cumul = udtf._cvState.getCumulativeLoss();
            loss = (cumul - loss) / lines;
            println(trainingIteration + " " + loss + " " + cumul / (trainingIteration * lines));
            data.close();
        }
        println("model size=" + udtf._model.getSize());
        Assert.assertTrue("Last loss was greater than expected: " + loss, loss < lossThreshold);
    }

    private static String[] toStringArray(ArrayList<StringFeature> x) {
        final int size = x.size();
        final String[] ret = new String[size];
        for (int i = 0; i < size; i++) {
            ret[i] = x.get(i).toString();
        }
        return ret;
    }

    private static void println(String line) {
        if (DEBUG) {
            System.out.println(line);
        }
    }

}
