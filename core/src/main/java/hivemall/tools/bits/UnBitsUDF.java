package hivemall.tools.bits;

import hivemall.utils.hadoop.HiveUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;

@Description(name = "unbits",
        value = "_FUNC_(long[] bitset) - Returns an long array of the give bitset representation")
@UDFType(deterministic = true, stateful = false)
public final class UnBitsUDF extends GenericUDF {

    private ListObjectInspector listOI;
    private LongObjectInspector listElemLongOI;

    public UnBitsUDF() {}

    @Override
    public ObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if (argOIs.length != 1) {
            throw new UDFArgumentLengthException(
                "Expected 1 argument for _FUNC_(long[] bitset) but got " + argOIs.length
                        + " arguments");
        }
        this.listOI = HiveUtils.asListOI(argOIs[0]);
        this.listElemLongOI = HiveUtils.asLongOI(listOI.getListElementObjectInspector());

        return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
    }

    @Override
    public List<LongWritable> evaluate(DeferredObject[] args) throws HiveException {
        assert (args.length == 1);
        Object arg0 = args[0].get();
        if (arg0 == null) {
            return null;
        }
        long[] longs = HiveUtils.asLongArray(arg0, listOI, listElemLongOI);
        BitSet bitset = BitSet.valueOf(longs);

        int size = bitset.cardinality();
        final List<LongWritable> list = new ArrayList<LongWritable>(size);
        for (int i = bitset.nextSetBit(0); i >= 0; i = bitset.nextSetBit(i + 1)) {
            list.add(new LongWritable(i));
        }
        return list;
    }


    @Override
    public String getDisplayString(String[] args) {
        return "unbits(" + Arrays.toString(args) + ")";
    }
}
