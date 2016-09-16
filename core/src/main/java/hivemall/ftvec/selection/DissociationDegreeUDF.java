package hivemall.ftvec.selection;

import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.lang.Preconditions;
import hivemall.utils.math.StatsUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import javax.annotation.Nonnull;

@Description(name = "",
        value = "_FUNC_(array<number> expected, array<number> observed) - Returns dissociation degree as double")
public abstract class DissociationDegreeUDF extends GenericUDF {
    private ListObjectInspector expectedOI;
    private DoubleObjectInspector expectedElOI;
    private ListObjectInspector observedOI;
    private DoubleObjectInspector observedElOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] OIs) throws UDFArgumentException {
        if (OIs.length != 2) {
            throw new UDFArgumentLengthException("Specify two arguments.");
        }

        if (!HiveUtils.isListOI(OIs[0])
                || !HiveUtils.isNumberOI(((ListObjectInspector) OIs[0]).getListElementObjectInspector())){
            throw new UDFArgumentTypeException(0, "Only array<number> type argument is acceptable but "
                    + OIs[0].getTypeName() + " was passed as `expected`");
        }

        if (!HiveUtils.isListOI(OIs[1])
                || !HiveUtils.isNumberOI(((ListObjectInspector) OIs[1]).getListElementObjectInspector())){
            throw new UDFArgumentTypeException(1, "Only array<number> type argument is acceptable but "
                    + OIs[1].getTypeName() + " was passed as `observed`");
        }

        expectedOI = (ListObjectInspector) OIs[0];
        expectedElOI = (DoubleObjectInspector) expectedOI.getListElementObjectInspector();
        observedOI = (ListObjectInspector) OIs[1];
        observedElOI = (DoubleObjectInspector) observedOI.getListElementObjectInspector();

        return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    }

    @Override
    public Object evaluate(GenericUDF.DeferredObject[] dObj) throws HiveException {
        final double[] expected = HiveUtils.asDoubleArray(dObj[0].get(),expectedOI,expectedElOI);
        final double[] observed = HiveUtils.asDoubleArray(dObj[1].get(),observedOI,observedElOI);

        Preconditions.checkNotNull(expected);
        Preconditions.checkNotNull(observed);
        Preconditions.checkArgument(expected.length == observed.length);

        final double dissociation = calcDissociation(expected,observed);

        return new DoubleWritable(dissociation);
    }

    @Override
    public String getDisplayString(String[] children) {
        final StringBuilder sb = new StringBuilder();
        sb.append(getFuncName());
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

    abstract double calcDissociation(@Nonnull final double[] expected,@Nonnull final  double[] observed);

    @Nonnull
    abstract String getFuncName();
}
