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
package hivemall.smile.classification;

import static org.junit.Assert.assertEquals;
import hivemall.smile.classification.DecisionTree.Node;
import hivemall.smile.data.Attribute;
import hivemall.smile.tools.TreePredictByJavascriptUDF;
import hivemall.smile.utils.SmileExtUtils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.text.ParseException;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.junit.Assert;
import org.junit.Test;

import smile.data.AttributeDataset;
import smile.data.parser.ArffParser;
import smile.math.Math;
import smile.validation.LOOCV;

public class DecisionTreeTest {
    private static final boolean DEBUG = false;

    /**
     * Test of learn method, of class DecisionTree.
     * 
     * @throws ParseException
     * @throws IOException
     */
    @Test
    public void testWeather() throws IOException, ParseException {
        URL url = new URL(
            "https://gist.githubusercontent.com/myui/2c9df50db3de93a71b92/raw/3f6b4ecfd4045008059e1a2d1c4064fb8a3d372a/weather.nominal.arff");
        InputStream is = new BufferedInputStream(url.openStream());

        ArffParser arffParser = new ArffParser();
        arffParser.setResponseIndex(4);

        AttributeDataset weather = arffParser.parse(is);
        double[][] x = weather.toArray(new double[weather.size()][]);
        int[] y = weather.toArray(new int[weather.size()]);

        int n = x.length;
        LOOCV loocv = new LOOCV(n);
        int error = 0;
        for (int i = 0; i < n; i++) {
            double[][] trainx = Math.slice(x, loocv.train[i]);
            int[] trainy = Math.slice(y, loocv.train[i]);

            Attribute[] attrs = SmileExtUtils.convertAttributeTypes(weather.attributes());
            DecisionTree tree = new DecisionTree(attrs, trainx, trainy, 3);
            if (y[loocv.test[i]] != tree.predict(x[loocv.test[i]]))
                error++;
        }

        debugPrint("Decision Tree error = " + error);
        assertEquals(5, error);
    }

    @Test
    public void testIris() throws IOException, ParseException {
        URL url = new URL(
            "https://gist.githubusercontent.com/myui/143fa9d05bd6e7db0114/raw/500f178316b802f1cade6e3bf8dc814a96e84b1e/iris.arff");
        InputStream is = new BufferedInputStream(url.openStream());

        ArffParser arffParser = new ArffParser();
        arffParser.setResponseIndex(4);

        AttributeDataset iris = arffParser.parse(is);
        double[][] x = iris.toArray(new double[iris.size()][]);
        int[] y = iris.toArray(new int[iris.size()]);

        int n = x.length;
        LOOCV loocv = new LOOCV(n);
        int error = 0;
        for (int i = 0; i < n; i++) {
            double[][] trainx = Math.slice(x, loocv.train[i]);
            int[] trainy = Math.slice(y, loocv.train[i]);

            Attribute[] attrs = SmileExtUtils.convertAttributeTypes(iris.attributes());
            DecisionTree tree = new DecisionTree(attrs, trainx, trainy, 4);
            if (y[loocv.test[i]] != tree.predict(x[loocv.test[i]]))
                error++;
        }

        debugPrint("Decision Tree error = " + error);
        assertEquals(7, error);
    }

    @Test
    public void testIris2() throws IOException, ParseException, HiveException {
        URL url = new URL(
            "https://gist.githubusercontent.com/myui/143fa9d05bd6e7db0114/raw/500f178316b802f1cade6e3bf8dc814a96e84b1e/iris.arff");
        InputStream is = new BufferedInputStream(url.openStream());

        ArffParser arffParser = new ArffParser();
        arffParser.setResponseIndex(4);
        AttributeDataset iris = arffParser.parse(is);
        double[][] x = iris.toArray(new double[iris.size()][]);
        int[] y = iris.toArray(new int[iris.size()]);

        int n = x.length;
        LOOCV loocv = new LOOCV(n);
        for (int i = 0; i < n; i++) {
            double[][] trainx = Math.slice(x, loocv.train[i]);
            int[] trainy = Math.slice(y, loocv.train[i]);

            Attribute[] attrs = SmileExtUtils.convertAttributeTypes(iris.attributes());
            DecisionTree tree = new DecisionTree(attrs, trainx, trainy, 4);
            assertEquals(tree.predict(x[loocv.test[i]]), evalPredict(tree, x[loocv.test[i]]));
        }
    }

    @Test
    public void testIrisSerializedObj() throws IOException, ParseException, HiveException {
        URL url = new URL(
            "https://gist.githubusercontent.com/myui/143fa9d05bd6e7db0114/raw/500f178316b802f1cade6e3bf8dc814a96e84b1e/iris.arff");
        InputStream is = new BufferedInputStream(url.openStream());

        ArffParser arffParser = new ArffParser();
        arffParser.setResponseIndex(4);
        AttributeDataset iris = arffParser.parse(is);
        double[][] x = iris.toArray(new double[iris.size()][]);
        int[] y = iris.toArray(new int[iris.size()]);

        int n = x.length;
        LOOCV loocv = new LOOCV(n);
        for (int i = 0; i < n; i++) {
            double[][] trainx = Math.slice(x, loocv.train[i]);
            int[] trainy = Math.slice(y, loocv.train[i]);

            Attribute[] attrs = SmileExtUtils.convertAttributeTypes(iris.attributes());
            DecisionTree tree = new DecisionTree(attrs, trainx, trainy, 4);

            byte[] b = tree.predictSerCodegen(false);
            Node node = DecisionTree.deserializeNode(b, b.length, false);
            assertEquals(tree.predict(x[loocv.test[i]]), node.predict(x[loocv.test[i]]));
        }
    }

    @Test
    public void testIrisSerializeObjCompressed() throws IOException, ParseException, HiveException {
        URL url = new URL(
            "https://gist.githubusercontent.com/myui/143fa9d05bd6e7db0114/raw/500f178316b802f1cade6e3bf8dc814a96e84b1e/iris.arff");
        InputStream is = new BufferedInputStream(url.openStream());

        ArffParser arffParser = new ArffParser();
        arffParser.setResponseIndex(4);
        AttributeDataset iris = arffParser.parse(is);
        double[][] x = iris.toArray(new double[iris.size()][]);
        int[] y = iris.toArray(new int[iris.size()]);

        int n = x.length;
        LOOCV loocv = new LOOCV(n);
        for (int i = 0; i < n; i++) {
            double[][] trainx = Math.slice(x, loocv.train[i]);
            int[] trainy = Math.slice(y, loocv.train[i]);

            Attribute[] attrs = SmileExtUtils.convertAttributeTypes(iris.attributes());
            DecisionTree tree = new DecisionTree(attrs, trainx, trainy, 4);

            byte[] b1 = tree.predictSerCodegen(true);
            byte[] b2 = tree.predictSerCodegen(false);
            Assert.assertTrue("b1.length = " + b1.length + ", b2.length = " + b2.length,
                b1.length < b2.length);
            Node node = DecisionTree.deserializeNode(b1, b1.length, true);
            assertEquals(tree.predict(x[loocv.test[i]]), node.predict(x[loocv.test[i]]));
        }
    }

    private static int evalPredict(DecisionTree tree, double[] x) throws HiveException, IOException {
        String script = tree.predictJsCodegen();
        debugPrint(script);
        TreePredictByJavascriptUDF udf = new TreePredictByJavascriptUDF();
        udf.initialize(new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector)});
        IntWritable result = (IntWritable) udf.evaluate(script, x, true);
        result = (IntWritable) udf.evaluate(script, x, true);
        udf.close();
        return result.get();
    }

    private static void debugPrint(String msg) {
        if (DEBUG) {
            System.out.println(msg);
        }
    }

}
