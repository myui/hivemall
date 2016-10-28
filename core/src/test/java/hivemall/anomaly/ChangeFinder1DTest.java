/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package hivemall.anomaly;

import hivemall.anomaly.ChangeFinderUDF.LossFunction;
import hivemall.anomaly.ChangeFinderUDF.Parameters;

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

public class ChangeFinder1DTest {
    private static final boolean DEBUG = false;

    @Test
    public void testCf1d() throws IOException, HiveException {
        Parameters params = new Parameters();
        params.set(LossFunction.logloss);
        PrimitiveObjectInspector oi = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        ChangeFinder1D cf = new ChangeFinder1D(params, oi);
        double[] outScores = new double[2];

        BufferedReader reader = readFile("cf1d.csv");
        println("x outlier change");
        String line;
        int numOutliers = 0, numChangepoints = 0;
        while ((line = reader.readLine()) != null) {
            double x = Double.parseDouble(line);
            cf.update(x, outScores);
            printf("%f %f %f%n", x, outScores[0], outScores[1]);
            if (outScores[0] > 10.d) {
                numOutliers++;
            }
            if (outScores[1] > 10.d) {
                numChangepoints++;
            }
        }
        Assert.assertTrue("#outliers SHOULD be greater than 10: " + numOutliers, numOutliers > 10);
        Assert.assertTrue("#outliers SHOULD be less than 20: " + numOutliers, numOutliers < 20);
        Assert.assertTrue("#changepoints SHOULD be greater than 0: " + numChangepoints,
            numChangepoints > 0);
        Assert.assertTrue("#changepoints SHOULD be less than 5: " + numChangepoints,
            numChangepoints < 5);
    }

    @Test
    public void testTwitterData() throws IOException, HiveException {
        Parameters params = new Parameters();
        params.set(LossFunction.logloss);
        params.r1 = 0.01d;
        params.k = 6;
        params.T1 = 10;
        params.T2 = 5;
        PrimitiveObjectInspector oi = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        ChangeFinder1D cf = new ChangeFinder1D(params, oi);
        double[] outScores = new double[2];

        BufferedReader reader = readFile("twitter.csv.gz");
        println("# time x outlier change");
        String line;
        int i = 1, numOutliers = 0, numChangepoints = 0;
        while ((line = reader.readLine()) != null) {
            double x = Double.parseDouble(line);
            cf.update(x, outScores);
            printf("%d %f %f %f%n", i, x, outScores[0], outScores[1]);
            if (outScores[0] > 30.d) {
                numOutliers++;
            }
            if (outScores[1] > 8.d) {
                numChangepoints++;
            }
            i++;
        }
        Assert.assertTrue("#outliers SHOULD be greater than 5: " + numOutliers, numOutliers > 5);
        Assert.assertTrue("#outliers SHOULD be less than 10: " + numOutliers, numOutliers < 10);
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
        InputStream is = ChangeFinder1DTest.class.getResourceAsStream(fileName);
        if (fileName.endsWith(".gz")) {
            is = new GZIPInputStream(is);
        }
        return new BufferedReader(new InputStreamReader(is));
    }

}
