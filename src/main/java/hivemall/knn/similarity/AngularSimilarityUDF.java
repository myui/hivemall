package hivemall.knn.similarity;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.FloatWritable;

/**
 * @see http://en.wikipedia.org/wiki/Cosine_similarity#Angular_similarity
 */
@Description(name = "angular_similarity", value = "_FUNC_(ftvec1, ftvec2) - Returns an angular similarity of the given two vectors")
@UDFType(deterministic = true, stateful = false)
public final class AngularSimilarityUDF extends UDF {

    public FloatWritable evaluate(List<String> ftvec1, List<String> ftvec2) {
        return new FloatWritable(angularSimilarity(ftvec1, ftvec2));
    }

    public static float angularSimilarity(final List<String> ftvec1, final List<String> ftvec2) {
        float cosineSim = CosineSimilarityUDF.cosineSimilarity(ftvec1, ftvec2);
        return 1.0f - (float) (Math.acos(cosineSim) / Math.PI);
    }

}
