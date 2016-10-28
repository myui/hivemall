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
import hivemall.utils.lang.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.zip.GZIPInputStream;

import javax.annotation.Nonnull;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well19937c;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Test;

public class ChangeFinder2DTest {
    private static final boolean DEBUG = false;

    @Test
    public void testCf1d() throws IOException, HiveException {
        Parameters params = new Parameters();
        params.set(LossFunction.logloss);
        PrimitiveObjectInspector oi = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        ListObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(oi);
        ChangeFinder2D cf = new ChangeFinder2D(params, listOI);
        double[] outScores = new double[2];
        List<Double> x = new ArrayList<Double>(1);

        BufferedReader reader = readFile("cf1d.csv");
        println("x outlier change");
        String line;
        int i = 1, numOutliers = 0, numChangepoints = 0;
        while ((line = reader.readLine()) != null) {
            double d = Double.parseDouble(line);
            x.add(Double.valueOf(d));

            cf.update(x, outScores);
            printf("%d %f %f %f%n", i, d, outScores[0], outScores[1]);
            if (outScores[0] > 10.d) {
                numOutliers++;
            }
            if (outScores[1] > 10.d) {
                numChangepoints++;
            }
            x.clear();
            i++;
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
        ListObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(oi);
        ChangeFinder2D cf = new ChangeFinder2D(params, listOI);
        double[] outScores = new double[2];
        List<Double> x = new ArrayList<Double>(1);

        BufferedReader reader = readFile("twitter.csv.gz");
        println("time x outlier change");
        String line;
        int i = 1, numOutliers = 0, numChangepoints = 0;
        while ((line = reader.readLine()) != null) {
            double d = Double.parseDouble(line);
            x.add(Double.valueOf(d));

            cf.update(x, outScores);
            printf("%d %f %f %f%n", i, d, outScores[0], outScores[1]);
            if (outScores[0] > 30.d) {
                numOutliers++;
            }
            if (outScores[1] > 8.d) {
                numChangepoints++;
            }

            x.clear();
            i++;
        }
        Assert.assertTrue("#outliers SHOULD be greater than 5: " + numOutliers, numOutliers > 5);
        Assert.assertTrue("#outliers SHOULD be less than 10: " + numOutliers, numOutliers < 10);
        Assert.assertTrue("#changepoints SHOULD be greater than 0: " + numChangepoints,
            numChangepoints > 0);
        Assert.assertTrue("#changepoints SHOULD be less than 5: " + numChangepoints,
            numChangepoints < 5);
    }

    @Test
    public void testPoissenDist() throws HiveException {
        final int examples = 10000;
        final int dims = 3;
        final PoissonDistribution[] poisson = new PoissonDistribution[] {
                new PoissonDistribution(10.d), new PoissonDistribution(5.d),
                new PoissonDistribution(20.d)};
        final Random rand = new Random(42);
        final Double[] x = new Double[dims];
        final List<Double> xList = Arrays.asList(x);

        Parameters params = new Parameters();
        params.set(LossFunction.logloss);
        params.r1 = 0.01d;
        params.k = 6;
        params.T1 = 10;
        params.T2 = 5;
        PrimitiveObjectInspector oi = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        ListObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(oi);
        final ChangeFinder2D cf = new ChangeFinder2D(params, listOI);
        final double[] outScores = new double[2];

        println("# time x0 x1 x2 outlier change");
        for (int i = 0; i < examples; i++) {
            double r = rand.nextDouble();
            x[0] = r * poisson[0].sample();
            x[1] = r * poisson[1].sample();
            x[2] = r * poisson[2].sample();

            cf.update(xList, outScores);
            printf("%d %f %f %f %f %f%n", i, x[0], x[1], x[2], outScores[0], outScores[1]);
        }
    }

    //@Test
    public void testSota5D() throws HiveException {
        final int DIM = 5;
        final int EXAMPLES = 20001;

        final Double[] x = new Double[DIM];
        final List<Double> xList = Arrays.asList(x);

        Parameters params = new Parameters();
        params.set(LossFunction.logloss);
        params.r1 = 0.01d;
        params.k = 10;
        params.T1 = 10;
        params.T2 = 10;
        PrimitiveObjectInspector oi = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        ListObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(oi);
        final ChangeFinder2D cf = new ChangeFinder2D(params, listOI);
        final double[] outScores = new double[2];

        RandomGenerator rng1 = new Well19937c(31L);
        final UniformIntegerDistribution uniform = new UniformIntegerDistribution(rng1, 0, 10);
        RandomGenerator rng2 = new Well19937c(41L);
        final PoissonDistribution poissonEvent = new PoissonDistribution(rng2, 1000.d,
            PoissonDistribution.DEFAULT_EPSILON, PoissonDistribution.DEFAULT_MAX_ITERATIONS);
        final StringBuilder buf = new StringBuilder(256);

        println("# time x0 x1 x2 x3 x4 mean0 mean1 mean2 mean3 mean4 outlier change");
        FIN: for (int i = 0; i < EXAMPLES;) {
            int len = poissonEvent.sample();
            double data[][] = new double[DIM][len];
            double mean[] = new double[DIM];
            double sd[] = new double[DIM];
            for (int j = 0; j < DIM; j++) {
                mean[j] = uniform.sample() * 5.d;
                sd[j] = uniform.sample() / 10.d * 5.d + 1.d;
                if (i % 5 == 0) {
                    mean[j] += 50.d;
                }
                NormalDistribution normDist = new NormalDistribution(new Well19937c(i + j),
                    mean[j], sd[j]);
                data[j] = normDist.sample(len);
                data[j][len / (j + 2) + DIM % (j + 1)] = mean[j] + (j + 4) * sd[j];
            }
            for (int j = 0; j < len; j++) {
                if (i >= EXAMPLES) {
                    break FIN;
                }
                x[0] = data[0][j];
                x[1] = data[1][j];
                x[2] = data[2][j];
                x[3] = data[3][j];
                x[4] = data[4][j];
                cf.update(xList, outScores);
                buf.append(i)
                   .append(' ')
                   .append(x[0].doubleValue())
                   .append(' ')
                   .append(x[1].doubleValue())
                   .append(' ')
                   .append(x[2].doubleValue())
                   .append(' ')
                   .append(x[3].doubleValue())
                   .append(' ')
                   .append(x[4].doubleValue())
                   .append(' ')
                   .append(mean[0])
                   .append(' ')
                   .append(mean[1])
                   .append(' ')
                   .append(mean[2])
                   .append(' ')
                   .append(mean[3])
                   .append(' ')
                   .append(mean[4])
                   .append(' ')
                   .append(outScores[0])
                   .append(' ')
                   .append(outScores[1]);
                println(buf.toString());
                StringUtils.clear(buf);
                i++;
            }
        }
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
