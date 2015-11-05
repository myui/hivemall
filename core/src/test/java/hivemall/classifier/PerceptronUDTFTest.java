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
import hivemall.io.FeatureValue;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

public class PerceptronUDTFTest {

    @Test
    public void testInitialize() throws UDFArgumentException {
        PerceptronUDTF udtf = new PerceptronUDTF();
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
    public void testUpdate() throws UDFArgumentException {
        PerceptronUDTF udtf = new PerceptronUDTF();
        ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ListObjectInspector stringListOI = ObjectInspectorFactory.getStandardListObjectInspector(stringOI);
        udtf.initialize(new ObjectInspector[] { stringListOI,
                PrimitiveObjectInspectorFactory.javaIntObjectInspector });

        /* update weights by List<Object> */
        FeatureValue word1 = FeatureValue.parse("good");
        FeatureValue word2 = FeatureValue.parse("opinion");
        FeatureValue[] features1 = new FeatureValue[] { word1, word2 };
        udtf.update(features1, 1, 0.f);

        /* check weights */
        assertEquals(1.f, udtf.model.get(word1.getFeature()).get(), 1e-5f);
        assertEquals(1.f, udtf.model.get(word2.getFeature()).get(), 1e-5f);

        /* update weights by List<Object> */
        FeatureValue word3 = FeatureValue.parse("bad");
        FeatureValue word4 = FeatureValue.parse("opinion");
        FeatureValue[] features2 = new FeatureValue[] { word3, word4 };
        udtf.update(features2, -1, 0.f);

        /* check weights */
        assertEquals(1.f, udtf.model.get(word1.getFeature()).get(), 1e-5f);
        assertEquals(-1.f, udtf.model.get(word3.getFeature()).get(), 1e-5f);
        assertEquals(0.f, udtf.model.get(word4.getFeature()).get(), 1e-5f);
    }

    @Test
    public void testUpdateStringTypeDisableBias() throws UDFArgumentException {
        PerceptronUDTF udtf = new PerceptronUDTF();
        ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ListObjectInspector stringListOI = ObjectInspectorFactory.getStandardListObjectInspector(stringOI);
        ObjectInspector param = ObjectInspectorUtils.getConstantObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector, "");
        udtf.initialize(new ObjectInspector[] { stringListOI,
                PrimitiveObjectInspectorFactory.javaIntObjectInspector, param });

        /* update weights by List<Object> */
        FeatureValue word1 = FeatureValue.parse("good");
        FeatureValue word2 = FeatureValue.parse("opinion");
        FeatureValue[] features1 = new FeatureValue[] { word1, word2 };
        udtf.update(features1, 1, 0.f);

        /* check weights */
        assertEquals(1.f, udtf.model.get(word1.getFeature()).get(), 1e-5f);
        assertEquals(1.f, udtf.model.get(word2.getFeature()).get(), 1e-5f);

        /* update weights by List<Object> */
        FeatureValue word3 = FeatureValue.parse("bad");
        FeatureValue word4 = FeatureValue.parse("opinion");
        FeatureValue[] features2 = new FeatureValue[] { word3, word4 };
        udtf.update(features2, -1, 0.f);

        /* check weights */
        assertEquals(1.f, udtf.model.get(word1.getFeature()).get(), 1e-5f);
        assertEquals(-1.f, udtf.model.get(word3.getFeature()).get(), 1e-5f);
        assertEquals(0.f, udtf.model.get(word4.getFeature()).get(), 1e-5f);
    }
}
