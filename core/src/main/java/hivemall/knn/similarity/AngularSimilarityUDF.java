package hivemall.knn.similarity;

import hivemall.utils.hadoop.HiveUtils;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.FloatWritable;

/**
 * @see http://en.wikipedia.org/wiki/Cosine_similarity#Angular_similarity
 */
@Description(name = "angular_similarity", value = "_FUNC_(ftvec1, ftvec2) - Returns an angular similarity of the given two vectors")
@UDFType(deterministic = true, stateful = false)
public final class AngularSimilarityUDF extends GenericUDF {

    private ListObjectInspector arg0ListOI, arg1ListOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if(argOIs.length != 2) {
            throw new UDFArgumentException("angular_similarity takes 2 arguments");
        }
        this.arg0ListOI = HiveUtils.asListOI(argOIs[0]);
        this.arg1ListOI = HiveUtils.asListOI(argOIs[1]);

        return PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
    }

    @Override
    public FloatWritable evaluate(DeferredObject[] arguments) throws HiveException {
        List<String> ftvec1 = HiveUtils.asStringList(arguments[0], arg0ListOI);
        List<String> ftvec2 = HiveUtils.asStringList(arguments[1], arg1ListOI);
        float similarity = angularSimilarity(ftvec1, ftvec2);
        return new FloatWritable(similarity);
    }

    public static float angularSimilarity(final List<String> ftvec1, final List<String> ftvec2) {
        float cosineSim = CosineSimilarityUDF.cosineSimilarity(ftvec1, ftvec2);
        return 1.0f - (float) (Math.acos(cosineSim) / Math.PI);
    }

    @Override
    public String getDisplayString(String[] children) {
        return "angular_similarity(" + Arrays.toString(children) + ")";
    }

}
