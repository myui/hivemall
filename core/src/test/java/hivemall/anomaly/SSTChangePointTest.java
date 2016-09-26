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

import hivemall.anomaly.SSTChangePointUDF.Parameters;

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

public class SSTChangePointTest {
    private static final boolean DEBUG = false;

    @Test
    public void testSST() throws IOException, HiveException {
        Parameters params = new Parameters();
        PrimitiveObjectInspector oi = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        SSTChangePoint sst = new SSTChangePoint(params, oi);
        double[] outScores = new double[1];

        BufferedReader reader = readFile("cf1d.csv");
        println("x change");
        String line;
        int numChangepoints = 0;
        while ((line = reader.readLine()) != null) {
            double x = Double.parseDouble(line);
            sst.update(x, outScores);
            printf("%f %f%n", x, outScores[0]);
            if (outScores[0] > 0.95d) {
                numChangepoints++;
            }
        }
        Assert.assertTrue("#changepoints SHOULD be greater than 0: " + numChangepoints,
            numChangepoints > 0);
        Assert.assertTrue("#changepoints SHOULD be less than 5: " + numChangepoints,
            numChangepoints < 5);
    }

    @Test
    public void testTwitterData() throws IOException, HiveException {
        Parameters params = new Parameters();
        PrimitiveObjectInspector oi = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        SSTChangePoint sst = new SSTChangePoint(params, oi);
        double[] outScores = new double[1];

        BufferedReader reader = readFile("twitter.csv.gz");
        println("# time x change");
        String line;
        int i = 1, numChangepoints = 0;
        while ((line = reader.readLine()) != null) {
            double x = Double.parseDouble(line);
            sst.update(x, outScores);
            printf("%d %f %f%n", i, x, outScores[0]);
            if (outScores[0] > 0.005d) {
                numChangepoints++;
            }
            i++;
        }
        Assert.assertTrue("#changepoints SHOULD be greater than 0: " + numChangepoints,
            numChangepoints > 0);
        Assert.assertTrue("#changepoints SHOULD be less than 5: " + numChangepoints,
            numChangepoints < 5);
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
        InputStream is = SSTChangePointTest.class.getResourceAsStream(fileName);
        if (fileName.endsWith(".gz")) {
            is = new GZIPInputStream(is);
        }
        return new BufferedReader(new InputStreamReader(is));
    }

}
