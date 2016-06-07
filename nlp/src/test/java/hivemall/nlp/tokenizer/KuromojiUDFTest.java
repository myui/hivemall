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
package hivemall.nlp.tokenizer;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;
import org.objenesis.strategy.StdInstantiatorStrategy;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

public class KuromojiUDFTest {

    @Test
    public void testOneArgment() throws UDFArgumentException, IOException {
        GenericUDF udf = new KuromojiUDF();
        ObjectInspector[] argOIs = new ObjectInspector[1];
        // line
        argOIs[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        udf.initialize(argOIs);
        udf.close();
    }

    @Test
    public void testTwoArgment() throws UDFArgumentException, IOException {
        GenericUDF udf = new KuromojiUDF();
        ObjectInspector[] argOIs = new ObjectInspector[2];
        // line
        argOIs[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        // mode
        argOIs[1] = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
            PrimitiveCategory.STRING, null);
        udf.initialize(argOIs);
        udf.close();
    }

    public void testExpectedMode() throws UDFArgumentException, IOException {
        GenericUDF udf = new KuromojiUDF();
        ObjectInspector[] argOIs = new ObjectInspector[2];
        // line
        argOIs[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        // mode
        argOIs[1] = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
            PrimitiveCategory.STRING, new Text("normal"));
        udf.initialize(argOIs);
        udf.close();
    }

    @Test(expected = UDFArgumentException.class)
    public void testInvalidMode() throws UDFArgumentException, IOException {
        GenericUDF udf = new KuromojiUDF();
        ObjectInspector[] argOIs = new ObjectInspector[2];
        // line
        argOIs[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        // mode
        argOIs[1] = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
            PrimitiveCategory.STRING, new Text("unsupported mode"));
        udf.initialize(argOIs);
        udf.close();
    }

    @Test
    public void testThreeArgment() throws UDFArgumentException, IOException {
        GenericUDF udf = new KuromojiUDF();
        ObjectInspector[] argOIs = new ObjectInspector[3];
        // line
        argOIs[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        // mode
        argOIs[1] = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
            PrimitiveCategory.STRING, null);
        // stopWords
        argOIs[2] = ObjectInspectorFactory.getStandardConstantListObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, null);
        udf.initialize(argOIs);
        udf.close();
    }

    @Test
    public void testFourArgment() throws UDFArgumentException, IOException {
        GenericUDF udf = new KuromojiUDF();
        ObjectInspector[] argOIs = new ObjectInspector[4];
        // line
        argOIs[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        // mode
        argOIs[1] = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
            PrimitiveCategory.STRING, null);
        // stopWords
        argOIs[2] = ObjectInspectorFactory.getStandardConstantListObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, null);
        // stopTags
        argOIs[3] = ObjectInspectorFactory.getStandardConstantListObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, null);
        udf.initialize(argOIs);
        udf.close();
    }

    @Test
    public void testEvalauteOneRow() throws IOException, HiveException {
        KuromojiUDF udf = new KuromojiUDF();
        ObjectInspector[] argOIs = new ObjectInspector[1];
        // line
        argOIs[0] = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        udf.initialize(argOIs);

        DeferredObject[] args = new DeferredObject[1];
        args[0] = new DeferredObject() {
            public Text get() throws HiveException {
                return new Text("クロモジのJapaneseAnalyzerを使ってみる。テスト。");
            }
        };
        List<Text> tokens = udf.evaluate(args);
        Assert.assertNotNull(tokens);
        Assert.assertEquals(5, tokens.size());
        udf.close();
    }

    @Test
    public void testEvalauteTwoRows() throws IOException, HiveException {
        KuromojiUDF udf = new KuromojiUDF();
        ObjectInspector[] argOIs = new ObjectInspector[1];
        // line
        argOIs[0] = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        udf.initialize(argOIs);

        DeferredObject[] args = new DeferredObject[1];
        args[0] = new DeferredObject() {
            public Text get() throws HiveException {
                return new Text("クロモジのJapaneseAnalyzerを使ってみる。テスト。");
            }
        };
        List<Text> tokens = udf.evaluate(args);
        Assert.assertNotNull(tokens);
        Assert.assertEquals(5, tokens.size());

        args[0] = new DeferredObject() {
            public Text get() throws HiveException {
                return new Text("クロモジのJapaneseAnalyzerを使ってみる。");
            }
        };
        tokens = udf.evaluate(args);
        Assert.assertNotNull(tokens);
        Assert.assertEquals(4, tokens.size());

        udf.close();
    }

    @Test
    public void testSerializeByKryo() throws UDFArgumentException {
        final KuromojiUDF udf = new KuromojiUDF();
        ObjectInspector[] argOIs = new ObjectInspector[1];
        argOIs[0] = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        udf.initialize(argOIs);

        Kryo kryo = new Kryo();
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
        Output output = new Output(1024 * 16);
        kryo.writeObject(output, udf);
        output.close();
    }
}
