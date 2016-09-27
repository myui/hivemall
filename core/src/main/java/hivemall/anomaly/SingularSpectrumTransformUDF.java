/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hivemall.anomaly;

import hivemall.UDFWithOptions;
import hivemall.utils.collections.DoubleRingBuffer;
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.lang.Preconditions;
import hivemall.utils.lang.Primitives;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

@Description(
        name = "sst",
        value = "_FUNC_(double|array<double> x [, const string options])"
                + " - Returns change-point scores and decisions using Singular Spectrum Transformation (SST)."
                + " It will return a tuple <double changepoint_score [, boolean is_changepoint]>")
public final class SingularSpectrumTransformUDF extends UDFWithOptions {

    private transient Parameters _params;
    private transient SingularSpectrumTransform _sst;

    private transient double[] _scores;
    private transient Object[] _result;
    private transient DoubleWritable _changepointScore;
    @Nullable
    private transient BooleanWritable _isChangepoint = null;

    public SingularSpectrumTransformUDF() {}

    // Visible for testing
    Parameters getParameters() {
        return _params;
    }

    @Override
    protected Options getOptions() {
        Options opts = new Options();
        opts.addOption("w", "window", true, "Number of samples which affects change-point score [default: 30]");
        opts.addOption("n", "n_past", true,
            "Number of past windows for change-point scoring [default: equal to `w` = 30]");
        opts.addOption("m", "n_current", true,
            "Number of current windows for change-point scoring [default: equal to `w` = 30]");
        opts.addOption("g", "current_offset", true,
            "Offset of the current windows from the updating sample [default: `-w` = -30]");
        opts.addOption("r", "n_component", true,
            "Number of singular vectors (i.e. principal components) [default: 3]");
        opts.addOption("k", "n_dim", true,
            "Number of dimensions for the Krylov subspaces [default: 5 (`2*r` if `r` is even, `2*r-1` otherwise)]");
        opts.addOption("th", "threshold", true,
            "Score threshold (inclusive) for determining change-point existence [default: -1, do not output decision]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(String optionValues) throws UDFArgumentException {
        CommandLine cl = parseOptions(optionValues);

        this._params.w = Primitives.parseInt(cl.getOptionValue("w"), _params.w);
        this._params.n = Primitives.parseInt(cl.getOptionValue("n"), _params.w);
        this._params.m = Primitives.parseInt(cl.getOptionValue("m"), _params.w);
        this._params.g = Primitives.parseInt(cl.getOptionValue("g"), -1 * _params.w);
        this._params.r = Primitives.parseInt(cl.getOptionValue("r"), _params.r);
        this._params.k = Primitives.parseInt(
            cl.getOptionValue("k"), (_params.r % 2 == 0) ? (2 * _params.r) : (2 * _params.r - 1));
        this._params.changepointThreshold = Primitives.parseDouble(
            cl.getOptionValue("th"), _params.changepointThreshold);

        Preconditions.checkArgument(_params.w >= 2, "w must be greather than 1: " + _params.w);
        Preconditions.checkArgument(_params.r >= 1, "r must be greater than 0: " + _params.r);
        Preconditions.checkArgument(_params.k >= 1, "k must be greater than 0: " + _params.k);
        Preconditions.checkArgument(_params.changepointThreshold > 0.d && _params.changepointThreshold < 1.d,
            "changepointThreshold must be in range (0, 1): " + _params.changepointThreshold);

        return cl;
    }

    @Override
    public ObjectInspector initialize(@Nonnull ObjectInspector[] argOIs)
            throws UDFArgumentException {
        if (argOIs.length < 1 || argOIs.length > 2) {
            throw new UDFArgumentException(
                "_FUNC_(double|array<double> x [, const string options]) takes 1 or 2 arguments: "
                        + Arrays.toString(argOIs));
        }

        this._params = new Parameters();
        if (argOIs.length == 2) {
            String options = HiveUtils.getConstString(argOIs[1]);
            processOptions(options);
        }

        ObjectInspector argOI0 = argOIs[0];
        PrimitiveObjectInspector xOI = HiveUtils.asDoubleCompatibleOI(argOI0);
        this._sst = new SingularSpectrumTransform(_params, xOI);

        this._scores = new double[1];

        final Object[] result;
        final ArrayList<String> fieldNames = new ArrayList<String>();
        final ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("changepoint_score");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        if (_params.changepointThreshold != -1d) {
            fieldNames.add("is_changepoint");
            fieldOIs.add(PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);
            result = new Object[2];
            this._isChangepoint = new BooleanWritable(false);
            result[1] = _isChangepoint;
        } else {
            result = new Object[1];
        }
        this._changepointScore = new DoubleWritable(0.d);
        result[0] = _changepointScore;
        this._result = result;

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public Object[] evaluate(@Nonnull DeferredObject[] args) throws HiveException {
        Object x = args[0].get();
        if (x == null) {
            return _result;
        }

        _sst.update(x, _scores);

        double changepointScore = _scores[0];
        _changepointScore.set(changepointScore);
        if (_isChangepoint != null) {
            _isChangepoint.set(changepointScore >= _params.changepointThreshold);
        }

        return _result;
    }

    @Override
    public void close() throws IOException {
        this._result = null;
        this._changepointScore = null;
        this._isChangepoint = null;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "sst(" + Arrays.toString(children) + ")";
    }

    static final class Parameters {
        int w = 30;
        int n = 30;
        int m = 30;
        int g = -30;
        int r = 3;
        int k = 5;
        double changepointThreshold = -1.d;

        Parameters() {}
    }

    public interface SingularSpectrumTransformInterface {
        void update(@Nonnull Object arg, @Nonnull double[] outScores) throws HiveException;
    }

}
