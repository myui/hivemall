/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2016 Makoto YUI
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

package hivemall.ftvec;

import hivemall.utils.collections.ComparableList;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import hivemall.ftvec.OnehotEncodingUDAF.GenericUDAFOnehotEncodingEvaluator;
import hivemall.ftvec.OnehotEncodingUDAF.GenericUDAFOnehotEncodingEvaluator.EncodingBuffer;

import java.util.ArrayList;
import java.util.List;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ OnehotEncodingUDAF.class })
public class TestOnehotEncodingUDAF {
    EncodingBuffer buffer = null;
    @Before
    public void setup() {
        buffer = new EncodingBuffer(2);
        buffer.reset();
    }

    @After
    public void shutdown() {
        buffer.reset();
    }

    @Test
    public void testEncodingBufferSize() {
        ComparableList<String> features1 = new ComparableList<String>();
        features1.add("human");
        features1.add("mammal");
        buffer.add(features1);
        assertEquals(1, buffer.getFeaturesSet().size());

        ComparableList<String> features2 = new ComparableList<String>();
        features2.add("dog");
        features2.add("mammal");
        buffer.add(features2);
        assertEquals(2, buffer.getFeaturesSet().size());

        ComparableList<String> features3 = new ComparableList<String>();
        features3.add("dog");
        features3.add("mammal");
        buffer.add(features3);
        assertEquals(2, buffer.getFeaturesSet().size());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testEncodeFromBuffer() {
        ComparableList<String> features1 = new ComparableList<String>();
        features1.add("human");
        features1.add("mammal");
        buffer.add(features1);
        ComparableList<String> features2 = new ComparableList<String>();
        features2.add("dog");
        features2.add("mammal");
        buffer.add(features2);
        ComparableList<String> features3 = new ComparableList<String>();
        features3.add("cat");
        features3.add("mammal");
        buffer.add(features3);
        ComparableList<String> features4 = new ComparableList<String>();
        features4.add("wasp");
        features4.add("insect");
        buffer.add(features4);
        ComparableList<String> features5 = new ComparableList<String>();
        features5.add("human");
        features5.add("mammal");
        buffer.add(features5);

        List<Object[]> ret = (List<Object[]>)buffer.terminate();
        assertEquals(4, ret.size());
        for (Object[] encode : ret) {
            assertEquals(3, encode.length);
        }
        // Check encoding of A
        Object[] encodedA = ret.get(0);
        assertEquals(new Text("cat"), encodedA[0]);
        assertEquals(new Text("mammal"), encodedA[1]);
        ArrayList<String> encode = (ArrayList<String>)encodedA[2];
        assertEquals(2, encode.size());
        assertEquals(new Text("0:1"), encode.get(0));
        assertEquals(new Text("5:1"), encode.get(1));

        // Check encoding of A
        Object[] encodedB = ret.get(1);
        assertEquals(new Text("dog"), encodedB[0]);
        assertEquals(new Text("mammal"), encodedB[1]);
        encode = (ArrayList<String>)encodedB[2];
        assertEquals(2, encode.size());
        assertEquals(new Text("1:1"), encode.get(0));
        assertEquals(new Text("5:1"), encode.get(1));

        // Check encoding of A
        Object[] encodedC = ret.get(2);
        assertEquals(new Text("human"), encodedC[0]);
        assertEquals(new Text("mammal"), encodedC[1]);
        encode = (ArrayList<String>)encodedC[2];
        assertEquals(2, encode.size());
        assertEquals(new Text("2:1"), encode.get(0));
        assertEquals(new Text("5:1"), encode.get(1));
    }

    @Test
    public void testResetEncodingBuffer() {
        ComparableList<String> features1 = new ComparableList<String>();
        features1.add("human");
        features1.add("mammal");
        buffer.add(features1);
        assertEquals(1, buffer.getFeaturesSet().size());
        buffer.reset();
        assertEquals(0, buffer.getFeaturesSet().size());
    }

    @Test(expected = UDFArgumentTypeException.class)
    public void testGetEvaluatorOverParameter() throws SemanticException {
        OnehotEncodingUDAF udaf = new OnehotEncodingUDAF();
        TypeInfo[] parameters = new TypeInfo[2];
        udaf.getEvaluator(parameters);
    }

    @Test(expected = UDFArgumentTypeException.class)
    public void testGetEvaluatorWithNonPrimitiveParams() throws SemanticException {
        OnehotEncodingUDAF udaf = new OnehotEncodingUDAF();
        ListTypeInfo[] parameters = new ListTypeInfo[1];
        parameters[0] = new ListTypeInfo();
        parameters[0].setListElementTypeInfo(new PrimitiveTypeInfo());
        udaf.getEvaluator(parameters);
    }

    @Test(expected = UDFArgumentTypeException.class)
    public void testGetEvaluatorWithDouble() throws SemanticException {
        OnehotEncodingUDAF udaf = new OnehotEncodingUDAF();
        TypeInfo[] parameters = new TypeInfo[1];
        parameters[0] = new PrimitiveTypeInfo();
        ((PrimitiveTypeInfo)parameters[0]).setTypeName("double");
        udaf.getEvaluator(parameters);
    }

    @Test
    public void testEvaluatorPartialOutput() throws HiveException {
        GenericUDAFOnehotEncodingEvaluator evaluator = new GenericUDAFOnehotEncodingEvaluator();
        GenericUDAFEvaluator.Mode m = GenericUDAFEvaluator.Mode.PARTIAL1;
        ObjectInspector[] parameters = new ObjectInspector[1];
        parameters[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector ret = evaluator.init(m, parameters);
        assertTrue(ret instanceof StandardListObjectInspector);
    }

    @Test
    public void testEvaluatorFinalOutput() throws HiveException {
        GenericUDAFOnehotEncodingEvaluator evaluator = new GenericUDAFOnehotEncodingEvaluator();
        GenericUDAFEvaluator.Mode m = GenericUDAFEvaluator.Mode.FINAL;
        ObjectInspector[] parameters = new ObjectInspector[1];
        parameters[0] = ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.writableIntObjectInspector
        );
        ObjectInspector ret = evaluator.init(m, parameters);
        assertTrue(ret instanceof StandardListObjectInspector);
    }
}
