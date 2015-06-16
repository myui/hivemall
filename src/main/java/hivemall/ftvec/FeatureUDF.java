package hivemall.ftvec;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;

@Description(name = "feature", value = "_FUNC_(string feature, double weight) - Returns a feature string")
@UDFType(deterministic = true, stateful = false)
public final class FeatureUDF extends UDF {

    public Text evaluate(int feature, float weight) {
        return new Text(feature + ":" + weight);
    }

    public Text evaluate(int feature, double weight) {
        return new Text(feature + ":" + weight);
    }

    public Text evaluate(long feature, float weight) {
        return new Text(feature + ":" + weight);
    }

    public Text evaluate(long feature, double weight) {
        return new Text(feature + ":" + weight);
    }

    public Text evaluate(String feature, float weight) {
        if(feature == null) {
            return null;
        }
        return new Text(feature + ':' + weight);
    }

    public Text evaluate(String feature, double weight) {
        if(feature == null) {
            return null;
        }
        return new Text(feature + ':' + weight);
    }

}
