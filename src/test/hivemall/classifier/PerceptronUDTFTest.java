/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013
 *   National Institute of Advanced Industrial Science and Technology (AIST)
 *   Registration Number: H25PRO-1520
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package hivemall.classifier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import hivemall.common.FeatureValue;

import java.util.ArrayList;
import java.util.List;

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
        StructObjectInspector intListSOI = udtf.initialize(
            new ObjectInspector[]{intListOI, intOI});
        assertEquals("struct<feature:int,weight:float>", intListSOI.getTypeName());

        /* test for STRING_TYPE_NAME feature */
        ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ListObjectInspector stringListOI = ObjectInspectorFactory.getStandardListObjectInspector(stringOI);
        StructObjectInspector stringListSOI = udtf.initialize(
            new ObjectInspector[]{stringListOI, intOI});
        assertEquals("struct<feature:string,weight:float>", stringListSOI.getTypeName());

        /* test for BIGINT_TYPE_NAME feature */
        ObjectInspector longOI = PrimitiveObjectInspectorFactory.javaLongObjectInspector;
        ListObjectInspector longListOI = ObjectInspectorFactory.getStandardListObjectInspector(longOI);
        StructObjectInspector longListSOI = udtf.initialize(
            new ObjectInspector[]{longListOI, intOI});
        assertEquals("struct<feature:bigint,weight:float>", longListSOI.getTypeName());
    }

    @Test
    public void testUpdate() throws UDFArgumentException {
        PerceptronUDTF udtf = new PerceptronUDTF();
        ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ListObjectInspector stringListOI = ObjectInspectorFactory.getStandardListObjectInspector(stringOI);
        udtf.initialize(new ObjectInspector[]{
            stringListOI,
            PrimitiveObjectInspectorFactory.javaIntObjectInspector
        });

        /* update weights by List<Object> */
        List<String> features1 = new ArrayList<String>();
        features1.add("good");
        features1.add("opinion");
        udtf.update(features1, 1, 0.f);

        /* check weights */
        FeatureValue word1 = FeatureValue.parse(new String("good"), udtf.feature_hashing);
        assertEquals(1.f, udtf.weights.get(word1.getFeature()).get(), 1e-5f);

        FeatureValue word2 = FeatureValue.parse(new String("opinion"), udtf.feature_hashing);
        assertEquals(1.f, udtf.weights.get(word2.getFeature()).get(), 1e-5f);

        /* update weights by List<Object> */
        List<String> features2 = new ArrayList<String>();
        features2.add("bad");
        features2.add("opinion");
        udtf.update(features2, -1, 0.f);

        /* check weights */
        assertEquals(1.f, udtf.weights.get(word1.getFeature()).get(), 1e-5f);

        FeatureValue word3 = FeatureValue.parse(new String("bad"), udtf.feature_hashing);
        assertEquals(-1.f, udtf.weights.get(word3.getFeature()).get(), 1e-5f);

        FeatureValue word4 = FeatureValue.parse(new String("opinion"), udtf.feature_hashing);
        assertEquals(0.f, udtf.weights.get(word4.getFeature()).get(), 1e-5f);

        /* check bias: disabled */
        assertEquals(0.f, udtf.bias, 1e-5f);
        assertEquals("bias cluase is disabled by default", null, udtf.biasKey);
    }

    @Test
    public void testUpdateStringTypeDisableBias() throws UDFArgumentException {
        PerceptronUDTF udtf = new PerceptronUDTF();
        ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ListObjectInspector stringListOI = ObjectInspectorFactory.getStandardListObjectInspector(stringOI);
        ObjectInspector param = ObjectInspectorUtils.getConstantObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            new String("-b 0.1")
        );
        udtf.initialize(new ObjectInspector[]{
            stringListOI,
            PrimitiveObjectInspectorFactory.javaIntObjectInspector,
            param
        });

        /* update weights by List<Object> */
        List<String> features1 = new ArrayList<String>();
        features1.add("good");
        features1.add("opinion");
        udtf.update(features1, 1, 0.f);

        /* check weights */
        FeatureValue word1 = FeatureValue.parse(new String("good"), udtf.feature_hashing);
        assertEquals(1.f, udtf.weights.get(word1.getFeature()).get(), 1e-5f);

        FeatureValue word2 = FeatureValue.parse(new String("opinion"), udtf.feature_hashing);
        assertEquals(1.f, udtf.weights.get(word2.getFeature()).get(), 1e-5f);

        /* check bias: enabled */
        assertEquals(0.1f, udtf.bias, 1e-5f);
        assertNotNull("bias clause is enabled", udtf.biasKey);
        assertEquals(0.1f, udtf.weights.get(udtf.biasKey).get(), 1e-5f);

        /* update weights by List<Object> */
        List<String> features2 = new ArrayList<String>();
        features2.add("bad");
        features2.add("opinion");
        udtf.update(features2, -1, 0.f);

        /* check weights */
        assertEquals(1.f, udtf.weights.get(word1.getFeature()).get(), 1e-5f);

        FeatureValue word3 = FeatureValue.parse(new String("bad"), udtf.feature_hashing);
        assertEquals(-1.f, udtf.weights.get(word3.getFeature()).get(), 1e-5f);

        FeatureValue word4 = FeatureValue.parse(new String("opinion"), udtf.feature_hashing);
        assertEquals(0.f, udtf.weights.get(word4.getFeature()).get(), 1e-5f);

        /* check bias: enabled */
        assertEquals(0.1f, udtf.bias, 1e-5f);
        assertNotNull("bias clause is enabled", udtf.biasKey);
        assertEquals(0.f, udtf.weights.get(udtf.biasKey).get(), 1e-5f);
    }
}
