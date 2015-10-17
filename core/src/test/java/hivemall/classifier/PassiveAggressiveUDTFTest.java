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
import hivemall.io.PredictionResult;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

public class PassiveAggressiveUDTFTest {

    @Test
    public void testInitialize() throws UDFArgumentException {
        PassiveAggressiveUDTF udtf = new PassiveAggressiveUDTF();
        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ListObjectInspector intListOI = ObjectInspectorFactory.getStandardListObjectInspector(intOI);

        /* test for INT_TYPE_NAME feature */
        StructObjectInspector intListSOI = udtf.initialize(new ObjectInspector[] {
                intListOI, intOI });
        assertEquals("struct<feature:int,weight:float>", intListSOI.getTypeName());

        /* test for STRING_TYPE_NAME feature */
        ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ListObjectInspector stringListOI = ObjectInspectorFactory.getStandardListObjectInspector(stringOI);
        StructObjectInspector stringListSOI = udtf.initialize(new ObjectInspector[] {
                stringListOI, intOI });
        assertEquals("struct<feature:string,weight:float>", stringListSOI.getTypeName());

        /* test for BIGINT_TYPE_NAME feature */
        ObjectInspector longOI = PrimitiveObjectInspectorFactory.javaLongObjectInspector;
        ListObjectInspector longListOI = ObjectInspectorFactory.getStandardListObjectInspector(longOI);
        StructObjectInspector longListSOI = udtf.initialize(new ObjectInspector[] {
                longListOI, intOI });
        assertEquals("struct<feature:bigint,weight:float>", longListSOI.getTypeName());
    }

    @Test
    public void testTrain() throws HiveException {
        PassiveAggressiveUDTF udtf = new PassiveAggressiveUDTF();
        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ListObjectInspector intListOI = ObjectInspectorFactory.getStandardListObjectInspector(intOI);
        udtf.initialize(new ObjectInspector[] { intListOI, intOI });

        /* train weights by List<Object> */
        List<Integer> features1 = new ArrayList<Integer>();
        features1.add(1);
        features1.add(2);
        features1.add(3);
        udtf.train(features1, 1);

        /* check weights */
        assertEquals(0.3333333f, udtf.model.get(1).get(), 1e-5f);
        assertEquals(0.3333333f, udtf.model.get(2).get(), 1e-5f);
        assertEquals(0.3333333f, udtf.model.get(3).get(), 1e-5f);

        /* train weights by Object[] */
        List<?> features2 = (List<?>) intListOI.getList(new Object[] { 3, 4, 5 });
        udtf.train(features2, 1);

        /* check weights */
        assertEquals(0.3333333f, udtf.model.get(1).get(), 1e-5f);
        assertEquals(0.3333333f, udtf.model.get(2).get(), 1e-5f);
        assertEquals(0.5555555f, udtf.model.get(3).get(), 1e-5f);
        assertEquals(0.2222222f, udtf.model.get(4).get(), 1e-5f);
        assertEquals(0.2222222f, udtf.model.get(5).get(), 1e-5f);
    }

    @Test
    public void testEta() {
        PassiveAggressiveUDTF udtf = new PassiveAggressiveUDTF();
        float loss = 0.1f;

        PredictionResult margin1 = new PredictionResult(0.5f).squaredNorm(0.05f);
        float expectedLearningRate1 = 2.0f;
        assertEquals(expectedLearningRate1, udtf.eta(loss, margin1), 1e-5f);

        PredictionResult margin2 = new PredictionResult(0.5f).squaredNorm(0.01f);
        float expectedLearningRate2 = 10.0f;
        assertEquals(expectedLearningRate2, udtf.eta(loss, margin2), 1e-5f);
    }

    @Test
    public void testPA1Eta() throws UDFArgumentException {
        PassiveAggressiveUDTF udtf = new PassiveAggressiveUDTF.PA1();
        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ListObjectInspector intListOI = ObjectInspectorFactory.getStandardListObjectInspector(intOI);
        ObjectInspector param = ObjectInspectorUtils.getConstantObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector, "-c 3.0");

        /* do initialize() with aggressiveness parameter */
        udtf.initialize(new ObjectInspector[] { intListOI, intOI, param });
        float loss = 0.1f;

        PredictionResult margin1 = new PredictionResult(0.5f).squaredNorm(0.05f);
        float expectedLearningRate1 = 2.0f;
        assertEquals(expectedLearningRate1, udtf.eta(loss, margin1), 1e-5f);

        PredictionResult margin2 = new PredictionResult(0.5f).squaredNorm(0.01f);
        float expectedLearningRate2 = 3.0f;
        assertEquals(expectedLearningRate2, udtf.eta(loss, margin2), 1e-5f);
    }

    @Test
    public void testPA1EtaDefaultParameter() throws UDFArgumentException {
        PassiveAggressiveUDTF udtf = new PassiveAggressiveUDTF.PA1();
        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ListObjectInspector intListOI = ObjectInspectorFactory.getStandardListObjectInspector(intOI);

        udtf.initialize(new ObjectInspector[] { intListOI, intOI });
        float loss = 0.1f;

        PredictionResult margin = new PredictionResult(0.5f).squaredNorm(0.05f);
        float expectedLearningRate = 1.0f;
        assertEquals(expectedLearningRate, udtf.eta(loss, margin), 1e-5f);
    }

    @Test
    public void testPA1TrainWithoutParameter() throws UDFArgumentException {
        PassiveAggressiveUDTF udtf = new PassiveAggressiveUDTF.PA1();
        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ListObjectInspector intListOI = ObjectInspectorFactory.getStandardListObjectInspector(intOI);

        /* define aggressive parameter */
        udtf.initialize(new ObjectInspector[] { intListOI, intOI });

        /* train weights */
        List<?> features = (List<?>) intListOI.getList(new Object[] { 1, 2, 3 });
        udtf.train(features, 1);

        /* check weights */
        assertEquals(0.3333333f, udtf.model.get(1).get(), 1e-5f);
        assertEquals(0.3333333f, udtf.model.get(2).get(), 1e-5f);
        assertEquals(0.3333333f, udtf.model.get(3).get(), 1e-5f);
    }

    @Test
    public void testPA1TrainWithParameter() throws UDFArgumentException {
        PassiveAggressiveUDTF udtf = new PassiveAggressiveUDTF.PA1();
        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ListObjectInspector intListOI = ObjectInspectorFactory.getStandardListObjectInspector(intOI);

        ObjectInspector param = ObjectInspectorUtils.getConstantObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector, "-c 0.1");
        /* define aggressive parameter */
        udtf.initialize(new ObjectInspector[] { intListOI, intOI, param });

        /* train weights */
        List<?> features = (List<?>) intListOI.getList(new Object[] { 1, 2, 3 });
        udtf.train(features, 1);

        /* check weights */
        assertEquals(0.1000000f, udtf.model.get(1).get(), 1e-5f);
        assertEquals(0.1000000f, udtf.model.get(2).get(), 1e-5f);
        assertEquals(0.1000000f, udtf.model.get(3).get(), 1e-5f);
    }

    @Test
    public void testPA2EtaWithoutParameter() throws UDFArgumentException {
        PassiveAggressiveUDTF udtf = new PassiveAggressiveUDTF.PA2();
        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ListObjectInspector intListOI = ObjectInspectorFactory.getStandardListObjectInspector(intOI);

        /* do initialize() with aggressiveness parameter */
        udtf.initialize(new ObjectInspector[] { intListOI, intOI });
        float loss = 0.1f;

        PredictionResult margin1 = new PredictionResult(0.5f).squaredNorm(0.05f);
        float expectedLearningRate1 = 0.1818181f;
        assertEquals(expectedLearningRate1, udtf.eta(loss, margin1), 1e-5f);

        PredictionResult margin2 = new PredictionResult(0.5f).squaredNorm(0.01f);
        float expectedLearningRate2 = 0.1960784f;
        assertEquals(expectedLearningRate2, udtf.eta(loss, margin2), 1e-5f);
    }

    @Test
    public void testPA2EtaWithParameter() throws UDFArgumentException {
        PassiveAggressiveUDTF udtf = new PassiveAggressiveUDTF.PA2();
        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ListObjectInspector intListOI = ObjectInspectorFactory.getStandardListObjectInspector(intOI);
        ObjectInspector param = ObjectInspectorUtils.getConstantObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector, "-c 3.0");

        /* do initialize() with aggressiveness parameter */
        udtf.initialize(new ObjectInspector[] { intListOI, intOI, param });
        float loss = 0.1f;

        PredictionResult margin1 = new PredictionResult(0.5f).squaredNorm(0.05f);
        float expectedLearningRate1 = 0.4615384f;
        assertEquals(expectedLearningRate1, udtf.eta(loss, margin1), 1e-5f);

        PredictionResult margin2 = new PredictionResult(0.5f).squaredNorm(0.01f);
        float expectedLearningRate2 = 0.5660377f;
        assertEquals(expectedLearningRate2, udtf.eta(loss, margin2), 1e-5f);
    }

}
