package hivemall.ftvec.trans;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Test;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.verifyPrivate;

import hivemall.utils.hadoop.WritableUtils;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ BinarizeLabelUDTF.class })
public class TestBinarizeLabelUDTF {
    @Test(expected = UDFArgumentException.class)
    public void testInsufficientLabelColumn() throws HiveException {
        BinarizeLabelUDTF udtf = new BinarizeLabelUDTF();
        System.out.println(udtf);
        ObjectInspector[] argOIs = new ObjectInspector[2];
        argOIs[0] = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        List<String> featureNames = Arrays.asList("positive", "features");
        argOIs[1] = ObjectInspectorFactory.getStandardConstantListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector, featureNames);

        udtf.initialize(argOIs);
    }

    @Test
    public void test2Positive3NegativeSample() throws Exception {
        BinarizeLabelUDTF udtf = spy(new BinarizeLabelUDTF());
        System.out.println(udtf);
        ObjectInspector[] argOIs = new ObjectInspector[3];
        argOIs[0] = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        argOIs[1] = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        List<String> featureNames = Arrays.asList("positive", "negative", "features");
        argOIs[2] = ObjectInspectorFactory.getStandardConstantListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector, featureNames);

        doNothing().when(udtf, "forward", any());
        udtf.initialize(argOIs);

        Object[] arguments = new Object[3];
        arguments[0] = new Integer(2);
        arguments[1] = new Integer(3);
        arguments[2] = WritableUtils.val("a:1", "b:2");
        udtf.process(arguments);

        verifyPrivate(udtf, times(5)).invoke("forward", any(Object[].class));
        verifyPrivate(udtf, times(2)).invoke("forward", aryEq(new Object[] { WritableUtils.val("a:1", "b:2"), 0}));
        verifyPrivate(udtf, times(3)).invoke("forward", aryEq(new Object[] { WritableUtils.val("a:1", "b:2"), 1}));
    }

    @Test
    public void test0Positive0NegativeSample() throws Exception {
        BinarizeLabelUDTF udtf = spy(new BinarizeLabelUDTF());
        System.out.println(udtf);
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
