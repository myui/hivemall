package hivemall.tools.array;

import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.lang.Preconditions;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.IntWritable;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

@Description(name = "array_top_k_indices",
        value = "_FUNC_(array<number> array, const int k) - Returns indices array of top-k as array<int>")
public class ArrayTopKIndicesUDF extends GenericUDF {
    private ListObjectInspector arrayOI;
    private PrimitiveObjectInspector elementOI;
    private PrimitiveObjectInspector kOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] OIs) throws UDFArgumentException {
        if (OIs.length != 2) {
            throw new UDFArgumentLengthException("Specify two or three arguments.");
        }

        if (!HiveUtils.isNumberListOI(OIs[0])) {
            throw new UDFArgumentTypeException(0, "Only array<number> type argument is acceptable but "
                    + OIs[0].getTypeName() + " was passed as `array`");
        }
        if (!HiveUtils.isIntegerOI(OIs[1])) {
            throw new UDFArgumentTypeException(1, "Only int type argument is acceptable but "
                    + OIs[1].getTypeName() + " was passed as `k`");
        }

        arrayOI = HiveUtils.asListOI(OIs[0]);
        elementOI = HiveUtils.asDoubleCompatibleOI(arrayOI.getListElementObjectInspector());
        kOI = HiveUtils.asIntegerOI(OIs[1]);

        return ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.writableIntObjectInspector);
    }

    @Override
    public Object evaluate(GenericUDF.DeferredObject[] dObj) throws HiveException {
        final double[] array = HiveUtils.asDoubleArray(dObj[0].get(), arrayOI, elementOI);
        final int k = PrimitiveObjectInspectorUtils.getInt(dObj[1].get(), kOI);

        Preconditions.checkNotNull(array);
        Preconditions.checkArgument(array.length >= k);

        List<Map.Entry<Integer, Double>> list = new ArrayList<Map.Entry<Integer, Double>>();
        for (int i = 0; i < array.length; i++) {
            list.add(new AbstractMap.SimpleEntry<Integer, Double>(i, array[i]));
        }
        list.sort(new Comparator<Map.Entry<Integer, Double>>() {
            @Override
            public int compare(Map.Entry<Integer, Double> o1, Map.Entry<Integer, Double> o2) {
                return o1.getValue() > o2.getValue() ? -1 : 1;
            }
        });

        List<IntWritable> result = new ArrayList<IntWritable>();
        for (int i = 0; i < k; i++) {
            result.add(new IntWritable(list.get(i).getKey()));
        }
        return result;
    }

    @Override
    public String getDisplayString(String[] children) {
        StringBuilder sb = new StringBuilder();
        sb.append("array_top_k_indices");
        sb.append("(");
        if (children.length > 0) {
            sb.append(children[0]);
            for (int i = 1; i < children.length; i++) {
                sb.append(", ");
                sb.append(children[i]);
            }
        }
        sb.append(")");
        return sb.toString();
    }
}
