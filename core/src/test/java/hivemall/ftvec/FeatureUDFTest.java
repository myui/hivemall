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
package hivemall.ftvec;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FeatureUDFTest {
    FeatureUDF udf = null;

    @Before
    public void setup() {
        udf = new FeatureUDF();
    }

    @Test
    public void testIntInt() throws Exception {
        ObjectInspector featureOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector weightOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        udf.initialize(new ObjectInspector[]{featureOI, weightOI});

        Text ret = (Text)udf.evaluate(new GenericUDF.DeferredObject[]{
            new DeferredJavaObject(1),
            new DeferredJavaObject(2)
        });

        Assert.assertEquals("1:2", ret.toString());
    }

    @Test
    public void testIntLong() throws Exception {
        ObjectInspector featureOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector weightOI = PrimitiveObjectInspectorFactory.javaLongObjectInspector;
        udf.initialize(new ObjectInspector[]{featureOI, weightOI});

        Text ret = (Text)udf.evaluate(new GenericUDF.DeferredObject[]{
                new DeferredJavaObject(1),
                new DeferredJavaObject(2L)
        });

        Assert.assertEquals("1:2", ret.toString());
    }

    @Test
    public void testIntFloat() throws Exception {
        ObjectInspector featureOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector weightOI = PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
        udf.initialize(new ObjectInspector[]{featureOI, weightOI});

        Text ret = (Text)udf.evaluate(new GenericUDF.DeferredObject[]{
                new DeferredJavaObject(1),
                new DeferredJavaObject(2.5f)
        });

        Assert.assertEquals("1:2.5", ret.toString());
    }

    @Test
    public void testIntDouble() throws Exception {
        ObjectInspector featureOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector weightOI = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        udf.initialize(new ObjectInspector[]{featureOI, weightOI});

        Text ret = (Text)udf.evaluate(new GenericUDF.DeferredObject[]{
                new DeferredJavaObject(1),
                new DeferredJavaObject(2.5)
        });

        Assert.assertEquals("1:2.5", ret.toString());
    }


    @Test
    public void testLongInt() throws Exception {
        ObjectInspector featureOI = PrimitiveObjectInspectorFactory.javaLongObjectInspector;
        ObjectInspector weightOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        udf.initialize(new ObjectInspector[]{featureOI, weightOI});

        Text ret = (Text)udf.evaluate(new GenericUDF.DeferredObject[]{
                new DeferredJavaObject(1L),
                new DeferredJavaObject(2)
        });

        Assert.assertEquals("1:2", ret.toString());
    }

    @Test
    public void testLongLong() throws Exception {
        ObjectInspector featureOI = PrimitiveObjectInspectorFactory.javaLongObjectInspector;
        ObjectInspector weightOI = PrimitiveObjectInspectorFactory.javaLongObjectInspector;
        udf.initialize(new ObjectInspector[]{featureOI, weightOI});

        Text ret = (Text)udf.evaluate(new GenericUDF.DeferredObject[]{
                new DeferredJavaObject(1L),
                new DeferredJavaObject(2L)
        });

        Assert.assertEquals("1:2", ret.toString());
    }

    @Test
    public void testLongFloat() throws Exception {
        ObjectInspector featureOI = PrimitiveObjectInspectorFactory.javaLongObjectInspector;
        ObjectInspector weightOI = PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
        udf.initialize(new ObjectInspector[]{featureOI, weightOI});

        Text ret = (Text)udf.evaluate(new GenericUDF.DeferredObject[]{
                new DeferredJavaObject(1L),
                new DeferredJavaObject(2.5f)
        });

        Assert.assertEquals("1:2.5", ret.toString());
    }

    @Test
    public void testLongDouble() throws Exception {
        ObjectInspector featureOI = PrimitiveObjectInspectorFactory.javaLongObjectInspector;
        ObjectInspector weightOI = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        udf.initialize(new ObjectInspector[]{featureOI, weightOI});

        Text ret = (Text)udf.evaluate(new GenericUDF.DeferredObject[]{
                new DeferredJavaObject(1L),
                new DeferredJavaObject(2.5)
        });

        Assert.assertEquals("1:2.5", ret.toString());
    }

    @Test
    public void testStringInt() throws Exception {
        ObjectInspector featureOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector weightOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        udf.initialize(new ObjectInspector[]{featureOI, weightOI});

        Text ret = (Text)udf.evaluate(new GenericUDF.DeferredObject[]{
                new DeferredJavaObject("f1"),
                new DeferredJavaObject(2)
        });

        Assert.assertEquals("f1:2", ret.toString());
    }

    @Test
    public void testStringLong() throws Exception {
        ObjectInspector featureOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector weightOI = PrimitiveObjectInspectorFactory.javaLongObjectInspector;
        udf.initialize(new ObjectInspector[]{featureOI, weightOI});

        Text ret = (Text)udf.evaluate(new GenericUDF.DeferredObject[]{
                new DeferredJavaObject("f1"),
                new DeferredJavaObject(2L)
        });

        Assert.assertEquals("f1:2", ret.toString());
    }

    @Test
    public void testStringFloat() throws Exception {
        ObjectInspector featureOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector weightOI = PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
        udf.initialize(new ObjectInspector[]{featureOI, weightOI});

        Text ret = (Text)udf.evaluate(new GenericUDF.DeferredObject[]{
                new DeferredJavaObject("f1"),
                new DeferredJavaObject(2.5f)
        });

        Assert.assertEquals("f1:2.5", ret.toString());
    }

    @Test
    public void testStringDouble() throws Exception {
        ObjectInspector featureOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector weightOI = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        udf.initialize(new ObjectInspector[]{featureOI, weightOI});

        Text ret = (Text)udf.evaluate(new GenericUDF.DeferredObject[]{
                new DeferredJavaObject("f1"),
                new DeferredJavaObject(2.5)
        });

        Assert.assertEquals("f1:2.5", ret.toString());
    }

    @Test(expected = UDFArgumentException.class)
    public void testStringString() throws Exception {
        ObjectInspector featureOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector weightOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        udf.initialize(new ObjectInspector[]{featureOI, weightOI});
    }

    @Test
    public void testNullWeight() throws Exception {
        ObjectInspector featureOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector weightOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        udf.initialize(new ObjectInspector[]{featureOI, weightOI});

        Text ret = (Text)udf.evaluate(new GenericUDF.DeferredObject[] {
                new DeferredJavaObject("f1"),
                new DeferredJavaObject(null)
        });

        Assert.assertNull(ret);

    }
}
