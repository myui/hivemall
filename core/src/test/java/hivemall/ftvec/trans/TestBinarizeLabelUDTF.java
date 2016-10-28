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
package hivemall.ftvec.trans;

import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.verifyPrivate;
import hivemall.utils.hadoop.WritableUtils;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({BinarizeLabelUDTF.class})
public class TestBinarizeLabelUDTF {

    // ignored to avoid
    // org.apache.hadoop.hive.shims.ShimLoader.getMajorVersion(ShimLoader.java:141) ExceptionInInitializerError
    // in Hive v0.13.0
    //@Test(expected = UDFArgumentException.class)
    public void testInsufficientLabelColumn() throws HiveException {
        BinarizeLabelUDTF udtf = new BinarizeLabelUDTF();
        ObjectInspector[] argOIs = new ObjectInspector[2];
        argOIs[0] = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        List<String> featureNames = Arrays.asList("positive", "features");
        argOIs[1] = ObjectInspectorFactory.getStandardConstantListObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, featureNames);

        udtf.initialize(argOIs);
    }

    @Test
    public void test2Positive3NegativeSample() throws Exception {
        BinarizeLabelUDTF udtf = spy(new BinarizeLabelUDTF());
        ObjectInspector[] argOIs = new ObjectInspector[3];
        argOIs[0] = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        argOIs[1] = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        List<String> featureNames = Arrays.asList("positive", "negative", "features");
        argOIs[2] = ObjectInspectorFactory.getStandardConstantListObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, featureNames);

        doNothing().when(udtf, "forward", any());
        udtf.initialize(argOIs);

        Object[] arguments = new Object[3];
        arguments[0] = new Integer(2);
        arguments[1] = new Integer(3);
        arguments[2] = WritableUtils.val("a:1", "b:2");
        udtf.process(arguments);

        verifyPrivate(udtf, times(5)).invoke("forward", any(Object[].class));
        verifyPrivate(udtf, times(2)).invoke("forward",
            aryEq(new Object[] {WritableUtils.val("a:1", "b:2"), 1}));
        verifyPrivate(udtf, times(3)).invoke("forward",
            aryEq(new Object[] {WritableUtils.val("a:1", "b:2"), 0}));
    }

    @Test
    public void test0Positive0NegativeSample() throws Exception {
        BinarizeLabelUDTF udtf = spy(new BinarizeLabelUDTF());
        ObjectInspector[] argOIs = new ObjectInspector[3];
        argOIs[0] = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        argOIs[1] = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        List<String> featureNames = Arrays.asList("positive", "negative", "features");
        argOIs[2] = ObjectInspectorFactory.getStandardConstantListObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, featureNames);

        doNothing().when(udtf, "forward", any());
        udtf.initialize(argOIs);

        Object[] arguments = new Object[3];
        arguments[0] = new Integer(0);
        arguments[1] = new Integer(0);
        arguments[2] = WritableUtils.val("a:1", "b:2");
        udtf.process(arguments);

        verifyPrivate(udtf, times(0)).invoke("forward", any(Object[].class));
    }
}
