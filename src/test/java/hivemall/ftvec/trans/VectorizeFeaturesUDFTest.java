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
package hivemall.ftvec.trans;

import hivemall.utils.hadoop.WritableUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class VectorizeFeaturesUDFTest {

    @Test(expected = UDFArgumentException.class)
    public void testMismatch() throws HiveException, IOException {
        VectorizeFeaturesUDF udf = new VectorizeFeaturesUDF();
        ObjectInspector[] argOIs = new ObjectInspector[3];
        List<String> featureNames = Arrays.asList("a", "b", "c");
        argOIs[0] = ObjectInspectorFactory.getStandardConstantListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector, featureNames);
        argOIs[1] = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        argOIs[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        udf.initialize(argOIs);

        udf.close();
    }

    @Test
    public void testOneArgument() throws HiveException, IOException {
        VectorizeFeaturesUDF udf = new VectorizeFeaturesUDF();
        ObjectInspector[] argOIs = new ObjectInspector[2];
        List<String> featureNames = Arrays.asList("a");
        argOIs[0] = ObjectInspectorFactory.getStandardConstantListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector, featureNames);
        argOIs[1] = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        udf.initialize(argOIs);

        DeferredObject[] arguments = new DeferredObject[2];
        arguments[1] = new DeferredJavaObject(new Double(0.1));

        List<Text> actuals = udf.evaluate(arguments);
        //System.out.println(actuals);
        List<Text> expected = WritableUtils.val(new String[] { "a:0.1" });
        Assert.assertEquals(expected, actuals);

        udf.close();
    }

    @Test
    public void testTwoArguments() throws HiveException, IOException {
        VectorizeFeaturesUDF udf = new VectorizeFeaturesUDF();
        ObjectInspector[] argOIs = new ObjectInspector[3];
        List<String> featureNames = Arrays.asList("a", "b");
        argOIs[0] = ObjectInspectorFactory.getStandardConstantListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector, featureNames);
        argOIs[1] = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        argOIs[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        udf.initialize(argOIs);

        DeferredObject[] arguments = new DeferredObject[3];
        arguments[1] = new DeferredJavaObject(new Double(0.1));
        arguments[2] = new DeferredJavaObject("1.1");

        List<Text> actuals = udf.evaluate(arguments);
        //System.out.println(actuals);        
        List<Text> expected = WritableUtils.val("a:0.1", "b:1.1");
        Assert.assertEquals(expected, actuals);

        udf.close();
    }

    @Test
    public void testAvoidZeroWeight() throws HiveException, IOException {
        VectorizeFeaturesUDF udf = new VectorizeFeaturesUDF();
        ObjectInspector[] argOIs = new ObjectInspector[3];
        List<String> featureNames = Arrays.asList("a", "b");
        argOIs[0] = ObjectInspectorFactory.getStandardConstantListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector, featureNames);
        argOIs[1] = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        argOIs[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        udf.initialize(argOIs);

        DeferredObject[] arguments = new DeferredObject[3];
        arguments[1] = new DeferredJavaObject(new Double(0.1));
        arguments[2] = new DeferredJavaObject("0");

        List<Text> actuals = udf.evaluate(arguments);
        //System.out.println(actuals);        
        List<Text> expected = WritableUtils.val(new String[] { "a:0.1" });
        Assert.assertEquals(expected, actuals);

        udf.close();
    }

    @Test
    public void testBooleanWeight() throws HiveException, IOException {
        VectorizeFeaturesUDF udf = new VectorizeFeaturesUDF();
        ObjectInspector[] argOIs = new ObjectInspector[3];
        List<String> featureNames = Arrays.asList("a", "b");
        argOIs[0] = ObjectInspectorFactory.getStandardConstantListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector, featureNames);
        argOIs[1] = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        argOIs[2] = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
        udf.initialize(argOIs);

        DeferredObject[] arguments = new DeferredObject[3];
        arguments[1] = new DeferredJavaObject(new Double(0.1));
        arguments[2] = new DeferredJavaObject(new Boolean(false));

        List<Text> actuals = udf.evaluate(arguments);
        //System.out.println(actuals);        
        List<Text> expected = WritableUtils.val(new String[] { "a:0.1" });
        Assert.assertEquals(expected, actuals);

        arguments[2] = new DeferredJavaObject(new Boolean(true));
        actuals = udf.evaluate(arguments);
        //System.out.println(actuals);
        expected = WritableUtils.val("a:0.1", "b:1.0");
        Assert.assertEquals(expected, actuals);

        udf.close();
    }

    @Test
    public void testCategoricalVariable() throws HiveException, IOException {
        VectorizeFeaturesUDF udf = new VectorizeFeaturesUDF();
        ObjectInspector[] argOIs = new ObjectInspector[3];
        List<String> featureNames = Arrays.asList("a", "b");
        argOIs[0] = ObjectInspectorFactory.getStandardConstantListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector, featureNames);
        argOIs[1] = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        argOIs[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        udf.initialize(argOIs);

        DeferredObject[] arguments = new DeferredObject[3];
        arguments[1] = new DeferredJavaObject(new Double(0.1));
        arguments[2] = new DeferredJavaObject("dayofweek");

        List<Text> actuals = udf.evaluate(arguments);
        //System.out.println(actuals);        
        List<Text> expected = WritableUtils.val("a:0.1", "b#dayofweek");
        Assert.assertEquals(expected, actuals);

        arguments[2] = new DeferredJavaObject("1.0");
        actuals = udf.evaluate(arguments);
        //System.out.println(actuals);        
        expected = WritableUtils.val("a:0.1", "b:1.0");
        Assert.assertEquals(expected, actuals);

        arguments[2] = new DeferredJavaObject("1");
        actuals = udf.evaluate(arguments);
        //System.out.println(actuals);        
        expected = WritableUtils.val("a:0.1", "b:1.0");
        Assert.assertEquals(expected, actuals);

        arguments[2] = new DeferredJavaObject("0");
        actuals = udf.evaluate(arguments);
        //System.out.println(actuals);
        expected = WritableUtils.val(new String[] { "a:0.1" });
        Assert.assertEquals(expected, actuals);

        udf.close();
    }

}
