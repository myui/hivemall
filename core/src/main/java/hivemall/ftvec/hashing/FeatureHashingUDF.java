package hivemall.ftvec.hashing;

import hivemall.UDFWithOptions;
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.hashing.MurmurHash3;
import hivemall.utils.lang.Primitives;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

@Description(name = "feature_hashing",
        value = "_FUNC_(array<string> features [, const string options])"
                + " - returns a hashed feature vector in array<string>")
@UDFType(deterministic = true, stateful = false)
public final class FeatureHashingUDF extends UDFWithOptions {

    @Nullable
    private ListObjectInspector _listOI;
    private int _numFeatures = MurmurHash3.DEFAULT_NUM_FEATURES;

    @Nullable
    private List<Text> _returnObj;

    public FeatureHashingUDF() {}

    @Override
    public String getDisplayString(String[] children) {
        return "feature_hashing(" + Arrays.toString(children) + ')';
    }

    @Override
    protected Options getOptions() {
        Options opts = new Options();
        opts.addOption("features", "num_features", true,
            "The number of features [default: 16777217 (2^24)]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(@Nonnull String optionValue) throws UDFArgumentException {
        CommandLine cl = parseOptions(optionValue);

        this._numFeatures = Primitives.parseInt(cl.getOptionValue("num_features"), _numFeatures);
        return cl;
    }

    @Override
    public ObjectInspector initialize(@Nonnull ObjectInspector[] argOIs)
            throws UDFArgumentException {
        if (argOIs.length != 1 && argOIs.length != 2) {
            throw new UDFArgumentLengthException(
                "The feature_hashing function takes 1 or 2 arguments: " + argOIs.length);
        }
        ObjectInspector argOI0 = argOIs[0];
        this._listOI = HiveUtils.isListOI(argOI0) ? (ListObjectInspector) argOI0 : null;

        if (argOIs.length == 2) {
            String opts = HiveUtils.getConstString(argOIs[1]);
            processOptions(opts);
        }

        if (_listOI == null) {
            return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        } else {
            return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        }
    }

    @Override
    public Object evaluate(@Nonnull DeferredObject[] arguments) throws HiveException {
        final Object arg0 = arguments[0].get();
        if (arg0 == null) {
            return null;
        }

        if (_listOI == null) {
            return evaluateScalar(arg0);
        } else {
            return evaluateList(arg0);
        }
    }

    @Nonnull
    private Text evaluateScalar(@Nonnull final Object arg0) {
        String fv = arg0.toString();
        return new Text(featureHashing(fv, _numFeatures));
    }

    @Nonnull
    private List<Text> evaluateList(@Nonnull final Object arg0) {
        final int len = _listOI.getListLength(arg0);
        List<Text> list = _returnObj;
        if (list == null) {
            list = new ArrayList<Text>(len);
            this._returnObj = list;
        } else {
            list.clear();
        }

        final int numFeatures = _numFeatures;
        for (int i = 0; i < len; i++) {
            Object obj = _listOI.getListElement(arg0, i);
            if (obj == null) {
                continue;
            }
            String fv = obj.toString();
            Text t = new Text(featureHashing(fv, numFeatures));
            list.add(t);
        }

        return list;
    }

    @Nonnull
    private static String featureHashing(@Nonnull final String fv, final int numFeatures) {
        final int headPos = fv.indexOf(':');
        if (headPos == -1) {
            int h = mhash(fv, numFeatures);
            return String.valueOf(h);
        } else {
            final int tailPos = fv.lastIndexOf(':');
            if (headPos == tailPos) {
                String f = fv.substring(0, headPos);
                int h = mhash(f, numFeatures);
                String v = fv.substring(headPos);
                return h + v;
            } else {
                String field = fv.substring(0, headPos + 1);
                String f = fv.substring(headPos + 1, tailPos);
                int h = mhash(f, numFeatures);
                String v = fv.substring(tailPos);
                return field + h + v;
            }
        }
    }

    private static int mhash(@Nonnull final String word, final int numFeatures) {
        int r = MurmurHash3.murmurhash3_x86_32(word, 0, word.length(), 0x9747b28c) % numFeatures;
        if (r < 0) {
            r += numFeatures;
        }
        return r + 1;
    }

}
