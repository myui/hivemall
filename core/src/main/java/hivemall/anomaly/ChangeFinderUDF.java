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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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

@Description(
        name = "changefinder",
        value = "_FUNC_(double|array<double> x [, const string options])"
                + " - Returns outlier/change-point scores and decisions using ChangeFinder."
                + " It will return a tuple <double outlier_score, double changepoint_score [, boolean is_anomaly [, boolean is_changepoint]]")
public final class ChangeFinderUDF extends UDFWithOptions {

    private transient Parameters _params;
    private transient ChangeFinder _changeFinder;

    private transient double[] _scores;
    private transient Object[] _result;
    private transient DoubleWritable _outlierScore;
    private transient DoubleWritable _changepointScore;
    @Nullable
    private transient BooleanWritable _isOutlier = null;
    @Nullable
    private transient BooleanWritable _isChangepoint = null;

    public ChangeFinderUDF() {}

    // Visible for testing
    Parameters getParameters() {
        return _params;
    }

    @Override
    protected Options getOptions() {
        Options opts = new Options();
        opts.addOption("k", "AR", true, "The order of AR model (i.e., AR(k)) [default: 7]");
        opts.addOption("r1", "x_forget", true,
            "Discounting parameter for outlier detection [range: (0,1); default: 0.02]");
        opts.addOption("r2", "y_forget", true,
            "Discounting parameter for change-point detection [range: (0,1); default: 0.02]");
        opts.addOption("T1", "x_window", true,
            "Number of past samples to include for calculating outlier score [default: 7]");
        opts.addOption("T2", "y_window", true,
            "Number of past samples to include for calculating change-point score [default: 7]");
        opts.addOption(
            "outlier_threshold",
            "x_threshold",
            true,
            "Score threshold (inclusive) for determining outlier existence [default: -1, do not output decision]");
        opts.addOption(
            "changepoint_threshold",
            "y_threshold",
            true,
            "Score threshold (inclusive) for determining change-point existence [default: -1, do not output decision]");
        opts.addOption("loss1", "lossfunc1", true,
            "Loss function for outliter scoring [default: hellinger, logloss]");
        opts.addOption("loss2", "lossfunc2", true,
            "Loss function for change point scoring [default: hellinger, logloss]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(String optionValues) throws UDFArgumentException {
        CommandLine cl = parseOptions(optionValues);

        this._params.k = Primitives.parseInt(cl.getOptionValue("k"), _params.k);
        this._params.r1 = Primitives.parseDouble(cl.getOptionValue("r1"), _params.r1);
        this._params.r2 = Primitives.parseDouble(cl.getOptionValue("r2"), _params.r2);
        this._params.T1 = Primitives.parseInt(cl.getOptionValue("T1"), _params.T1);
        this._params.T2 = Primitives.parseInt(cl.getOptionValue("T2"), _params.T2);
        this._params.outlierThreshold = Primitives.parseDouble(
            cl.getOptionValue("outlier_threshold"), _params.outlierThreshold);
        this._params.changepointThreshold = Primitives.parseDouble(
            cl.getOptionValue("changepoint_threshold"), _params.changepointThreshold);
        this._params.lossFunc1 = LossFunction.resolve(cl.getOptionValue("lossfunc1",
            LossFunction.hellinger.name()));
        this._params.lossFunc2 = LossFunction.resolve(cl.getOptionValue("lossfunc2",
            LossFunction.hellinger.name()));

        Preconditions.checkArgument(_params.k >= 2, "K must be greater than 1: " + _params.k);
        Preconditions.checkArgument(_params.r1 > 0.d && _params.r1 < 1.d,
            "r1 must be in range (0,1): " + _params.r1);
        Preconditions.checkArgument(_params.r2 > 0.d && _params.r2 < 1.d,
            "r2 must be in range (0,1): " + _params.r2);
        Preconditions.checkArgument(_params.T1 >= 2, "T1 must be greather than 1: " + _params.T1);
        Preconditions.checkArgument(_params.T2 >= 2, "T2 must be greather than 1: " + _params.T2);

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
        if (HiveUtils.isListOI(argOI0)) {
            ListObjectInspector listOI = HiveUtils.asListOI(argOI0);
            this._changeFinder = new ChangeFinder2D(_params, listOI);
            throw new UnsupportedOperationException("2D x is not supported yet");
        } else if (HiveUtils.isNumberOI(argOI0)) {
            PrimitiveObjectInspector xOI = HiveUtils.asDoubleCompatibleOI(argOI0);
            this._changeFinder = new ChangeFinder1D(_params, xOI);
        }

        this._scores = new double[2];

        final Object[] result;
        final ArrayList<String> fieldNames = new ArrayList<String>();
        final ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("outlier_score");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        fieldNames.add("changepoint_score");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        if (_params.outlierThreshold != -1d) {
            fieldNames.add("is_outlier");
            fieldOIs.add(PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);
            this._isOutlier = new BooleanWritable(false);
            if (_params.changepointThreshold != -1d) {
                fieldNames.add("is_changepoint");
                fieldOIs.add(PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);
                result = new Object[4];
                this._isChangepoint = new BooleanWritable(false);
                result[3] = _isChangepoint;
            } else {
                result = new Object[3];
            }
            result[2] = _isOutlier;
        } else {
            result = new Object[2];
        }
        this._outlierScore = new DoubleWritable(0d);
        result[0] = _outlierScore;
        this._changepointScore = new DoubleWritable(0d);
        result[1] = _changepointScore;
        this._result = result;

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public Object[] evaluate(@Nonnull DeferredObject[] args) throws HiveException {
        Object x = args[0].get();
        if (x == null) {
            return _result;
        }

        _changeFinder.update(x, _scores);

        double outlierScore = _scores[0];
        double changepointScore = _scores[1];
        _outlierScore.set(outlierScore);
        _changepointScore.set(changepointScore);
        if (_isOutlier != null) {
            _isOutlier.set(outlierScore >= _params.outlierThreshold);
            if (_isChangepoint != null) {
                _isChangepoint.set(changepointScore >= _params.changepointThreshold);
            }
        }

        return _result;
    }

    @Override
    public void close() throws IOException {
        this._result = null;
        this._outlierScore = null;
        this._changepointScore = null;
        this._isOutlier = null;
        this._isChangepoint = null;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "changefinder(" + Arrays.toString(children) + ")";
    }

    static final class Parameters {
        int k = 7;
        double r1 = 0.02d;
        double r2 = 0.02d;
        int T1 = 7;
        int T2 = 7;
        double outlierThreshold = -1d;
        double changepointThreshold = -1d;
        LossFunction lossFunc1 = LossFunction.hellinger;
        LossFunction lossFunc2 = LossFunction.hellinger;

        Parameters() {}

        void set(@Nonnull LossFunction func) {
            this.lossFunc1 = func;
            this.lossFunc2 = func;
        }

    }

    public interface ChangeFinder {
        void update(@Nonnull Object arg, @Nonnull double[] outScores) throws HiveException;
    }

    static double smoothing(@Nonnull final DoubleRingBuffer scores) {
        double sum = 0.d;
        for (double score : scores.getRing()) {
            sum += score;
        }
        int size = scores.size();
        return sum / size;
    }

    public enum LossFunction {
        logloss, hellinger;

        static LossFunction resolve(@Nullable final String name) {
            if (logloss.name().equalsIgnoreCase(name)) {
                return logloss;
            } else if (hellinger.name().equalsIgnoreCase(name)) {
                return hellinger;
            } else {
                throw new IllegalArgumentException("Unsupported LossFunction: " + name);
            }
        }
    }

}
