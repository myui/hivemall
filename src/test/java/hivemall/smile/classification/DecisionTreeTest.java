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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.text.ParseException;

import javax.script.ScriptException;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

import smile.data.AttributeDataset;
import smile.data.parser.ArffParser;
import smile.math.Math;
import smile.validation.LOOCV;

public class DecisionTreeTest {

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

    private static int evalPredict(DecisionTree tree, double[] x) throws HiveException, IOException {
        String script = tree.predictCodegen();
        System.out.println(script);
        TreePredictTrustedUDF udf = new TreePredictTrustedUDF();
        udf.initialize(new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector) });
        IntWritable result = (IntWritable) udf.evaluate(script, x, true);
        result = (IntWritable) udf.evaluate(script, x, true);
        udf.close();
        return result.get();
    }
}
