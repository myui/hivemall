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
package hivemall.classifier;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Test;

import hivemall.model.PredictionResult;

public class KernelizedPassiveAggressiveUDTFTest {

    @Test
    public void testInitialize() throws UDFArgumentException {
        KernelizedPassiveAggressiveUDTF udtf = new KernelizedPassiveAggressiveUDTF();
        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ListObjectInspector intListOI =
                ObjectInspectorFactory.getStandardListObjectInspector(intOI);

        /* test for INT_TYPE_NAME feature */
        StructObjectInspector intListSOI =
                udtf.initialize(new ObjectInspector[] {intListOI, intOI});
        assertEquals("struct<feature:int,weight:float>", intListSOI.getTypeName());

        /* test for STRING_TYPE_NAME feature */
        ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ListObjectInspector stringListOI =
                ObjectInspectorFactory.getStandardListObjectInspector(stringOI);
        StructObjectInspector stringListSOI =
                udtf.initialize(new ObjectInspector[] {stringListOI, intOI});
        assertEquals("struct<feature:string,weight:float>", stringListSOI.getTypeName());

        /* test for BIGINT_TYPE_NAME feature */
        ObjectInspector longOI = PrimitiveObjectInspectorFactory.javaLongObjectInspector;
        ListObjectInspector longListOI =
                ObjectInspectorFactory.getStandardListObjectInspector(longOI);
        StructObjectInspector longListSOI =
                udtf.initialize(new ObjectInspector[] {longListOI, intOI});
        assertEquals("struct<feature:bigint,weight:float>", longListSOI.getTypeName());
    }

    @Test
    public void testTrain() throws HiveException {
        KernelizedPassiveAggressiveUDTF udtf = new KernelizedPassiveAggressiveUDTF();
        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ListObjectInspector intListOI =
                ObjectInspectorFactory.getStandardListObjectInspector(intOI);
        udtf.initialize(new ObjectInspector[] {intListOI, intOI});

        /* train weights by List<Object> */
        List<Integer> features1 = new ArrayList<Integer>();
        features1.add(1);
        features1.add(2);
        features1.add(3);
        udtf.train(features1, 1);

        /* check weights */
        assertEquals(0.3333333f, udtf.getAlpha(0), 1e-5f);

        /* train weights by Object[] */
        List<?> features2 = (List<?>) intListOI.getList(new Object[] {3, 4, 5});
        udtf.train(features2, -1);

        /* check weights */
        assertEquals(0.3333333f, udtf.getAlpha(0), 1e-5f);
        assertEquals(-0.7777777f, udtf.getAlpha(1), 1e-5f);
    }

    @Test
    public void testEta() {
        KernelizedPassiveAggressiveUDTF udtf = new KernelizedPassiveAggressiveUDTF();
        float loss = 0.1f;

        PredictionResult margin1 = new PredictionResult(0.5f).squaredNorm(0.05f);
        float expectedLearningRate1 = 2.0f;
        assertEquals(expectedLearningRate1, udtf.eta(loss, margin1), 1e-5f);

        PredictionResult margin2 = new PredictionResult(0.5f).squaredNorm(0.01f);
        float expectedLearningRate2 = 10.0f;
        assertEquals(expectedLearningRate2, udtf.eta(loss, margin2), 1e-5f);
    }

    @Test
    public void testKPA1Eta() throws UDFArgumentException {
        KernelizedPassiveAggressiveUDTF udtf = new KernelizedPassiveAggressiveUDTF.KPA1();
        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ListObjectInspector intListOI =
                ObjectInspectorFactory.getStandardListObjectInspector(intOI);
        ObjectInspector param = ObjectInspectorUtils.getConstantObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, "-c 3.0");

        /* do initialize() with aggressiveness parameter */
        udtf.initialize(new ObjectInspector[] {intListOI, intOI, param});
        float loss = 0.1f;

        PredictionResult margin1 = new PredictionResult(0.5f).squaredNorm(0.05f);
        float expectedLearningRate1 = 2.0f;
        assertEquals(expectedLearningRate1, udtf.eta(loss, margin1), 1e-5f);

        PredictionResult margin2 = new PredictionResult(0.5f).squaredNorm(0.01f);
        float expectedLearningRate2 = 3.0f;
        assertEquals(expectedLearningRate2, udtf.eta(loss, margin2), 1e-5f);
    }

    @Test
    public void testKPA1EtaDefaultParameter() throws UDFArgumentException {
        KernelizedPassiveAggressiveUDTF udtf = new KernelizedPassiveAggressiveUDTF.KPA1();
        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ListObjectInspector intListOI =
                ObjectInspectorFactory.getStandardListObjectInspector(intOI);

        udtf.initialize(new ObjectInspector[] {intListOI, intOI});
        float loss = 0.1f;

        PredictionResult margin = new PredictionResult(0.5f).squaredNorm(0.05f);
        float expectedLearningRate = 1.0f;
        assertEquals(expectedLearningRate, udtf.eta(loss, margin), 1e-5f);
    }

    @Test
    public void testKPA1TrainWithoutParameter() throws UDFArgumentException {
        KernelizedPassiveAggressiveUDTF udtf = new KernelizedPassiveAggressiveUDTF.KPA1();
        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ListObjectInspector intListOI =
                ObjectInspectorFactory.getStandardListObjectInspector(intOI);

        /* define aggressive parameter */
        udtf.initialize(new ObjectInspector[] {intListOI, intOI});

        /* train weights */
        List<?> features = (List<?>) intListOI.getList(new Object[] {1, 2, 3});
        udtf.train(features, 1);

        /* check weights */
        assertEquals(0.3333333f, udtf.getAlpha(0), 1e-5f);
    }

    @Test
    public void testKPA1TrainWithParameter() throws UDFArgumentException {
        KernelizedPassiveAggressiveUDTF udtf = new KernelizedPassiveAggressiveUDTF.KPA1();
        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ListObjectInspector intListOI =
                ObjectInspectorFactory.getStandardListObjectInspector(intOI);

        ObjectInspector param = ObjectInspectorUtils.getConstantObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, "-c 0.1");
        /* define aggressive parameter */
        udtf.initialize(new ObjectInspector[] {intListOI, intOI, param});

        /* train weights */
        List<?> features = (List<?>) intListOI.getList(new Object[] {1, 2, 3});
        udtf.train(features, 1);

        /* check weights */
        assertEquals(0.1000000f, udtf.getAlpha(0), 1e-5f);
    }

    @Test
    public void testKPA2EtaWithoutParameter() throws UDFArgumentException {
        KernelizedPassiveAggressiveUDTF udtf = new KernelizedPassiveAggressiveUDTF.KPA2();
        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ListObjectInspector intListOI =
                ObjectInspectorFactory.getStandardListObjectInspector(intOI);

        /* do initialize() with aggressiveness parameter */
        udtf.initialize(new ObjectInspector[] {intListOI, intOI});
        float loss = 0.1f;

        PredictionResult margin1 = new PredictionResult(0.5f).squaredNorm(0.05f);
        float expectedLearningRate1 = 0.1818181f;
        assertEquals(expectedLearningRate1, udtf.eta(loss, margin1), 1e-5f);

        PredictionResult margin2 = new PredictionResult(0.5f).squaredNorm(0.01f);
        float expectedLearningRate2 = 0.1960784f;
        assertEquals(expectedLearningRate2, udtf.eta(loss, margin2), 1e-5f);
    }

    @Test
    public void testKPA2EtaWithParameter() throws UDFArgumentException {
        KernelizedPassiveAggressiveUDTF udtf = new KernelizedPassiveAggressiveUDTF.KPA2();
        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ListObjectInspector intListOI =
                ObjectInspectorFactory.getStandardListObjectInspector(intOI);
        ObjectInspector param = ObjectInspectorUtils.getConstantObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, "-c 3.0");

        /* do initialize() with aggressiveness parameter */
        udtf.initialize(new ObjectInspector[] {intListOI, intOI, param});
        float loss = 0.1f;

        PredictionResult margin1 = new PredictionResult(0.5f).squaredNorm(0.05f);
        float expectedLearningRate1 = 0.4615384f;
        assertEquals(expectedLearningRate1, udtf.eta(loss, margin1), 1e-5f);

        PredictionResult margin2 = new PredictionResult(0.5f).squaredNorm(0.01f);
        float expectedLearningRate2 = 0.5660377f;
        assertEquals(expectedLearningRate2, udtf.eta(loss, margin2), 1e-5f);
    }

    @Test
    public void testOptions() throws UDFArgumentException {
        KernelizedPassiveAggressiveUDTF udtf = new KernelizedPassiveAggressiveUDTF();
        KernelizedPassiveAggressiveUDTF udtf2 = new KernelizedPassiveAggressiveUDTF();
        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ListObjectInspector intListOI =
                ObjectInspectorFactory.getStandardListObjectInspector(intOI);
        ObjectInspector param = ObjectInspectorUtils.getConstantObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, "-a 0.5 -d 3 -PKI");
        ObjectInspector param2 = ObjectInspectorUtils.getConstantObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, "-a 1.0 -d 2");
        udtf.initialize(new ObjectInspector[] {intListOI, intOI, param});
        udtf2.initialize(new ObjectInspector[] {intListOI, intOI, param2});

        /* train weights by List<Object> */
        List<Integer> features1 = new ArrayList<Integer>();
        features1.add(1);
        features1.add(2);
        features1.add(3);
        udtf.train(features1, 1);
        udtf2.train(features1, 1);

        /* check weights */
        assertEquals(0.3333333f, udtf.getAlpha(0), 1e-5f);
        assertEquals(0.3333333f, udtf2.getAlpha(0), 1e-5f);

        /* train weights by Object[] */
        List<?> features2 = (List<?>) intListOI.getList(new Object[] {3, 4, 5});
        udtf.train(features2, -1);
        udtf2.train(features2, -1);

        /* check weights */
        assertEquals(0.3333333f, udtf.getAlpha(0), 1e-5f);
        assertEquals(-0.7083333f, udtf.getAlpha(1), 1e-5f);
        assertEquals(0.3333333f, udtf2.getAlpha(0), 1e-5f);
        assertEquals(-0.7777777f, udtf2.getAlpha(1), 1e-5f);

    }

    @Test
    public void testLoss() throws UDFArgumentException {
        KernelizedPassiveAggressiveUDTF udtf = new KernelizedPassiveAggressiveUDTF();
        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ListObjectInspector intListOI =
                ObjectInspectorFactory.getStandardListObjectInspector(intOI);
        ObjectInspector param = ObjectInspectorUtils.getConstantObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, "");
        udtf.initialize(new ObjectInspector[] {intListOI, intOI, param});
    }

    @Test
    public void testNews20() throws UDFArgumentException, IOException, ParseException {
        KernelizedPassiveAggressiveUDTF udtf = new KernelizedPassiveAggressiveUDTF();
        KernelizedPassiveAggressiveUDTF udtfPKI = new KernelizedPassiveAggressiveUDTF();
        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ListObjectInspector stringListOI =
                ObjectInspectorFactory.getStandardListObjectInspector(stringOI);
        ObjectInspector param = ObjectInspectorUtils.getConstantObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, "");
        ObjectInspector paramPKI = ObjectInspectorUtils.getConstantObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, "-PKI");
        udtf.initialize(new ObjectInspector[] {stringListOI, intOI, param});
        udtfPKI.initialize(new ObjectInspector[] {stringListOI, intOI, paramPKI});

        BufferedReader news20 = new BufferedReader(
            new InputStreamReader(getClass().getResourceAsStream("news20-small.binary")));
        ArrayList<String> words = new ArrayList<String>();
        String line = news20.readLine();
        for (int i = 1; line != null; i++) {
            StringTokenizer tokens = new StringTokenizer(line, " ");
            int label = Integer.parseInt(tokens.nextToken());
            while (tokens.hasMoreTokens()) {
                words.add(tokens.nextToken());
            }
            Assert.assertFalse(words.isEmpty());
            udtf.train(words, label);
            udtfPKI.train(words, label);

            float loss = udtf.getLoss();
            float lossPKI = udtfPKI.getLoss();

            Assert.assertEquals("iter#" + i + " loss (" + loss + ") != lossPKI (" + lossPKI
                    + ") at Line:\n" + line + '\n',
                loss, lossPKI, 0.f);

            words.clear();
            line = news20.readLine();
        }
        news20.close();
    }

    @Test
    public void testTime() throws UDFArgumentException, IOException, ParseException {
        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ListObjectInspector stringListOI =
                ObjectInspectorFactory.getStandardListObjectInspector(stringOI);
        long start;
        long end;
        BufferedReader news20;
        String fileName = "news20-small.binary";//choose from news20-tiny.binary, -small.binary, or -medium.binary. Alternatively, make your own! Original news20.binary is online and easily searchable if needed.
        String line;

        KernelizedPassiveAggressiveUDTF udtfSplit = new KernelizedPassiveAggressiveUDTF();
        ObjectInspector paramSplit = ObjectInspectorUtils.getConstantObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, "");
        udtfSplit.initialize(new ObjectInspector[] {stringListOI, intOI, paramSplit});
        start = System.nanoTime();
        news20 = new BufferedReader(
            new InputStreamReader(getClass().getResourceAsStream(fileName)));
        line = news20.readLine();
        ArrayList<Float> losses = new ArrayList<Float>();
        for (int i = 1; line != null; i++) {
            int space = line.indexOf(' ');
            int len = line.length();
            if (space == -1 || len <= space + 1) {
                line = news20.readLine();
                continue;
            }
            int label = Integer.parseInt(line.substring(0, space));
            line = line.substring(space + 1);
            List<String> wordsSplit = Arrays.asList(line.split(" "));
            udtfSplit.train(wordsSplit, label);
            float loss = udtfSplit.getLoss();
            losses.add(loss);
            line = news20.readLine();
        }
        news20.close();
        end = System.nanoTime();
        long timeSplit = end - start;

        KernelizedPassiveAggressiveUDTF udtfPKISplit = new KernelizedPassiveAggressiveUDTF();
        ObjectInspector paramPKISplit = ObjectInspectorUtils.getConstantObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, "-PKI");
        udtfPKISplit.initialize(new ObjectInspector[] {stringListOI, intOI, paramPKISplit});
        start = System.nanoTime();
        news20 = new BufferedReader(
            new InputStreamReader(getClass().getResourceAsStream(fileName)));
        line = news20.readLine();
        ArrayList<Float> lossesPKI = new ArrayList<Float>();
        for (int i = 1; line != null; i++) {
            int space = line.indexOf(' ');
            int len = line.length();
            if (space == -1 || len <= space + 1) {
                line = news20.readLine();
                continue;
            }
            int label = Integer.parseInt(line.substring(0, space));
            line = line.substring(space + 1);
            List<String> wordsSplit = Arrays.asList(line.split(" "));
            udtfPKISplit.train(wordsSplit, label);
            float lossPKI = udtfPKISplit.getLoss();
            lossesPKI.add(lossPKI);
            line = news20.readLine();
        }
        news20.close();
        end = System.nanoTime();
        long timePKISplit = end - start;

        Assert.assertEquals("Loss values do not match", losses, lossesPKI);

        System.out.println("split " + timeSplit + " splitPKI " + timePKISplit);//can't really assert one to be smaller than the other because PKI speed depends strongly on the sparsity of the data
    }
}
