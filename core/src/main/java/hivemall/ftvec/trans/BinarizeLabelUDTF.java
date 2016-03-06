package hivemall.ftvec.trans;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

@Description(name = "binarize_label",
        value = "_FUNC_(int/long positive, int/long negative, ...) " +
                "- Returns positive/negative records that are represented " +
                "as (..., int label) where label is 0 or 1")
@UDFType(deterministic = true, stateful = false)
public class BinarizeLabelUDTF extends GenericUDTF {

    private PrimitiveObjectInspector positiveOI;
    private PrimitiveObjectInspector negativeOI;
    private Object[] positiveObjs;
    private Object[] negativeObjs;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs)
            throws UDFArgumentException {
        if (argOIs.length < 3) {
            throw new UDFArgumentException("binalize_label(int/long positive, " +
                    "int/long negative, *) takes at least three arguments");
        }

        if (!(argOIs[0] instanceof PrimitiveObjectInspector)) {
            throw new UDFArgumentException("Expected numeric type but got "
                    + argOIs[0].getTypeName());
        }

        if (!(argOIs[1] instanceof PrimitiveObjectInspector)) {
            throw new UDFArgumentException("Expected numeric type but got "
                    + argOIs[1].getTypeName());
        }

        this.positiveOI = (PrimitiveObjectInspector)argOIs[0];
        this.negativeOI = (PrimitiveObjectInspector)argOIs[1];
        this.positiveObjs = new Object[argOIs.length - 1];
        this.negativeObjs = new Object[argOIs.length - 1];

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();


        for (int i = 2; i < argOIs.length; i++) {
            fieldNames.add("c" + (i - 2));
            // Use negative label ObjectInspector here. OIs for positive
            // label and negative labels must be same.
            fieldOIs.add(argOIs[i]);
        }
        fieldNames.add("c" + (argOIs.length - 2));
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);

        return ObjectInspectorFactory
                .getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        final Object[] positiveObjs = this.positiveObjs;
        int positive = PrimitiveObjectInspectorUtils.getInt(args[0], positiveOI);
        for (int i = 0; i < positiveObjs.length - 1; i++) {
            positiveObjs[i] = args[i + 2];
        }
        // Forward positive label as 0
        positiveObjs[positiveObjs.length - 1] = 0;
        for (int i = 0; i < positive; i++) {
            forward(positiveObjs);
        }

        final Object[] negativeObjs = this.negativeObjs;
        int negative = PrimitiveObjectInspectorUtils.getInt(args[1], negativeOI);
        for (int i = 0; i < negativeObjs.length - 1; i++) {
            negativeObjs[i] = args[i + 2];
        }
        // Forward negative label as 1
        negativeObjs[negativeObjs.length - 1] = 1;
        for (int i = 0; i < negative; i++) {
            forward(negativeObjs);
        }
    }

    @Override
    public void close() throws HiveException {
        this.positiveObjs = null;
        this.negativeObjs = null;
    }
}
