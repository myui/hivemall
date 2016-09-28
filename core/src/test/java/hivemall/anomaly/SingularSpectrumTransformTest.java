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
package hivemall.anomaly;

import hivemall.anomaly.SingularSpectrumTransformUDF.ScoreFunction;
import hivemall.anomaly.SingularSpectrumTransformUDF.Parameters;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import javax.annotation.Nonnull;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Test;

public class SingularSpectrumTransformTest {
    private static final boolean DEBUG = false;

    @Test
    public void testSVDSST() throws IOException, HiveException {
        int numChangepoints = detectSST(ScoreFunction.svd, 0.95d);
        Assert.assertTrue("#changepoints SHOULD be greater than 0: " + numChangepoints,
            numChangepoints > 0);
        Assert.assertTrue("#changepoints SHOULD be less than 5: " + numChangepoints,
            numChangepoints < 5);
    }

    @Test
    public void testIKASST() throws IOException, HiveException {
        int numChangepoints = detectSST(ScoreFunction.ika, 0.65d);
        Assert.assertTrue("#changepoints SHOULD be greater than 0: " + numChangepoints,
                numChangepoints > 0);
        Assert.assertTrue("#changepoints SHOULD be less than 5: " + numChangepoints,
                numChangepoints < 5);
    }

    @Test
    public void testSVDTwitterData() throws IOException, HiveException {
        int numChangepoints = detectTwitterData(ScoreFunction.svd, 0.005d);
        Assert.assertTrue("#changepoints SHOULD be greater than 0: " + numChangepoints,
            numChangepoints > 0);
        Assert.assertTrue("#changepoints SHOULD be less than 5: " + numChangepoints,
            numChangepoints < 5);
    }

    @Test
    public void testIKATwitterData() throws IOException, HiveException {
        int numChangepoints = detectTwitterData(ScoreFunction.ika, 0.0175d);
        Assert.assertTrue("#changepoints SHOULD be greater than 0: " + numChangepoints,
                numChangepoints > 0);
        Assert.assertTrue("#changepoints SHOULD be less than 5: " + numChangepoints,
                numChangepoints < 5);
    }

    private static int detectSST(@Nonnull final ScoreFunction scoreFunc,
             @Nonnull final double threshold) throws IOException, HiveException {
        Parameters params = new Parameters();
        params.set(scoreFunc);
        PrimitiveObjectInspector oi = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        SingularSpectrumTransform sst = new SingularSpectrumTransform(params, oi);
        double[] outScores = new double[1];

        BufferedReader reader = readFile("cf1d.csv");
        println("x change");
        String line;
        int numChangepoints = 0;
        while ((line = reader.readLine()) != null) {
            double x = Double.parseDouble(line);
            sst.update(x, outScores);
            printf("%f %f%n", x, outScores[0]);
            if (outScores[0] > threshold) {
                numChangepoints++;
            }
        }

        return numChangepoints;
    }

    private static int detectTwitterData(@Nonnull final ScoreFunction scoreFunc,
             @Nonnull final double threshold) throws IOException, HiveException {
        Parameters params = new Parameters();
        params.set(scoreFunc);
        PrimitiveObjectInspector oi = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        SingularSpectrumTransform sst = new SingularSpectrumTransform(params, oi);
        double[] outScores = new double[1];

        BufferedReader reader = readFile("twitter.csv.gz");
        println("# time x change");
        String line;
        int i = 1, numChangepoints = 0;
        while ((line = reader.readLine()) != null) {
            double x = Double.parseDouble(line);
            sst.update(x, outScores);
            printf("%d %f %f%n", i, x, outScores[0]);
            if (outScores[0] > threshold) {
                numChangepoints++;
            }
            i++;
        }

        return numChangepoints;
    }

    private static void println(String msg) {
        if (DEBUG) {
            System.out.println(msg);
        }
    }

    private static void printf(String format, Object... args) {
        if (DEBUG) {
            System.out.printf(format, args);
        }
    }

    @Nonnull
    private static BufferedReader readFile(@Nonnull String fileName) throws IOException {
        InputStream is = SingularSpectrumTransformTest.class.getResourceAsStream(fileName);
        if (fileName.endsWith(".gz")) {
            is = new GZIPInputStream(is);
        }
        return new BufferedReader(new InputStreamReader(is));
    }

}
