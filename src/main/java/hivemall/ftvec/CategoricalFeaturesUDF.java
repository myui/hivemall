package hivemall.ftvec;

import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.Text;

@Description(name = "categorical_features", value = "_FUNC_(array<string> featureNames, ...) - Returns a feature vector array<string>")
@UDFType(deterministic = true, stateful = false)
public final class CategoricalFeaturesUDF extends GenericUDF {

    private String[] featureNames;
    private PrimitiveObjectInspector[] inputOIs;
    private List<Text> result;

    @Override
    public ObjectInspector initialize(@Nonnull final ObjectInspector[] argOIs)
            throws UDFArgumentException {
        final int numArgOIs = argOIs.length;
        if(numArgOIs < 2) {
            throw new UDFArgumentException("argOIs.length must be greater that or equals to 2: "
                    + numArgOIs);
        }
        this.featureNames = HiveUtils.getConstStringArray(argOIs[0]);
        int numFeatureNames = featureNames.length;
        if(numFeatureNames < 1) {
            throw new UDFArgumentException("#featureNames must be greater than or equals to 1: "
                    + numFeatureNames);
        }
        int numFeatures = numArgOIs - 1;
        if(numFeatureNames != numFeatures) {
            throw new UDFArgumentException("#featureNames '" + numFeatureNames
                    + "' != #arguments '" + numFeatures + "'");
        }

        this.inputOIs = new PrimitiveObjectInspector[numFeatures];
        for(int i = 0; i < numFeatures; i++) {
            ObjectInspector oi = argOIs[i + 1];
            inputOIs[i] = HiveUtils.asPrimitiveObjectInspector(oi);
        }
        this.result = new ArrayList<Text>(numFeatures);

        return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }

    @Override
    public List<Text> evaluate(@Nonnull final DeferredObject[] arguments) throws HiveException {
        result.clear();

        final int size = arguments.length - 1;
        for(int i = 0; i < size; i++) {
            Object argument = arguments[i + 1].get();
            if(argument == null) {
                return null;
            }

            PrimitiveObjectInspector oi = inputOIs[i];
            if(oi.getPrimitiveCategory() == PrimitiveCategory.STRING) {
                String s = PrimitiveObjectInspectorUtils.getString(argument, oi);
                if(StringUtils.isNumber(s) == false) {// categorical feature representation                    
                    String featureName = featureNames[i];
                    Text f = new Text(featureName + '#' + s);
                    result.add(f);
                    continue;
                }
            }
            final float v = PrimitiveObjectInspectorUtils.getFloat(argument, oi);
            if(v == 1.f) {
                String featureName = featureNames[i];
                Text f = new Text(featureName + "#T");
                result.add(f);
            } else if(v == 0.f || v == -1.f) {
                String featureName = featureNames[i];
                Text f = new Text(featureName + "#F");
                result.add(f);
            }
        }
        return result;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "categorical_features(" + Arrays.toString(children) + ")";
    }

}
