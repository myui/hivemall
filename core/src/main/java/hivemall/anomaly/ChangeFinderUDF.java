/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
 * Copyright (C) 2013-2015 National Institute of Advanced Industrial Science and Technology (AIST)
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
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.lang.Primitives;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;

import javax.annotation.Nonnull;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.BlockRealMatrix;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.UncorrelatedRandomVectorGenerator;
import org.apache.commons.math3.random.UniformRandomGenerator;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;

@Description(name = "cf_detect",
        value = "_FUNC_(array<double> x [, const string options]) - Returns anomaly/change-point scores and decisions")
public class ChangeFinderUDF extends UDFWithOptions {
    private ListObjectInspector xOI;
    private DoubleObjectInspector xContentOI;

    private int dimensions;
    private boolean firstCall;

    private RealVector x;
    private LinkedList<RealVector> xHistory;//history is ordered from newest to oldest, i.e. xHistory.getFirst() is from t-1.
    //mu
    private RealVector xMeanEstimate;
    //C0
    private DiagonalMatrix xCovar0;
    //Ci
    private RealMatrix[] xCovar;
    //Ai
    private RealMatrix[] xModelMatrix;
    //x-hat
    private RealVector xEstimate;
    //Sigma
    private RealMatrix xModelCovar;
    private int xRunningWindowSize;
    private double xForgetfulness;
    private double xThreshold;

    private double y;
    private double yRunningSum;
    private LinkedList<Double> xScoreHistory;
    private double yMeanEstimate;
    private double[] yCovar;
    private double[] yModelCoeff;
    private double yEstimate;
    private double yModelVar;
    private int yRunningWindowSize;
    private double yForgetfulness;
    private double yThreshold;


    @Override
    public String getDisplayString(String[] arg0) {
        return "_FUNC_";//TODO
    }

    @Override
    protected Options getOptions() {
        Options opts = new Options();
        opts.addOption("aWindow", "anomalyDetectionRunningWindowSize", true,
            "Number of past samples to include in anomaly detection calculation [default: 10]");
        opts.addOption("cWindow", "changePointDetectionRunningWindowSize", true,
            "Number of past samples to include in change-point detection calculation [default: 10]");
        opts.addOption("aForget", "anomalyDetectionForgetfulness", true,
            "Forgetfulness parameter for anomaly detection [range: [0,1]; default: 0.02]");
        opts.addOption("cForget", "changePointDetectionForgetfulness", true,
            "Forgetfulness parameter for change-point detection [range: [0,1]; default: 0.02]");
        opts.addOption("aThresh", "anomalyDetectionThreshold", true,
            "Score threshold for determining anomaly existence [default: 10.0]");
        opts.addOption("cThresh", "changePointDetectionThreshold", true,
            "Score threshold for determining change-point existence [default: 10.0]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(@Nonnull String optionValues) throws UDFArgumentException {
        int aWindow = 10;
        int cWindow = 10;
        double aForget = 0.02;
        double cForget = 0.02;
        double aThresh = 10.d;
        double cThresh = 10.d;

        CommandLine cl = parseOptions(optionValues);
        aWindow = Primitives.parseInt(cl.getOptionValue("aWindow"), aWindow);
        cWindow = Primitives.parseInt(cl.getOptionValue("cWindow"), cWindow);
        aForget = Primitives.parseDouble(cl.getOptionValue("aForget"), aForget);
        cForget = Primitives.parseDouble(cl.getOptionValue("cForget"), cForget);
        aThresh = Primitives.parseDouble(cl.getOptionValue("aThresh"), aThresh);
        cThresh = Primitives.parseDouble(cl.getOptionValue("cThresh"), cThresh);
        if (aWindow <= 1) {
            throw new UDFArgumentException("aWindow must be 2 or greater: " + aWindow);
        }
        if (cWindow <= 1) {
            throw new UDFArgumentException("cWindow must be 2 or greater: " + cWindow);
        }
        if (aForget < 0.d || aForget > 1.d) {
            throw new UDFArgumentException("aForget must be in the range [0,1]: " + aForget);
        }
        if (cForget < 0.d || cForget > 1.d) {
            throw new UDFArgumentException("cForget must be in the range [0,1]: " + cForget);
        }
        this.xRunningWindowSize = aWindow;
        this.yRunningWindowSize = cWindow;
        this.xForgetfulness = aForget;
        this.yForgetfulness = cForget;
        this.xThreshold = aThresh;
        this.yThreshold = cThresh;
        return cl;
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        int arguments = argOIs.length;
        if (!(arguments == 1 || arguments == 2)) {
            throw new UDFArgumentLengthException(getClass().getSimpleName()
                    + " takes 1 or 2 arguments: array<double> x [, CONSTANT STRING options]: "
                    + Arrays.toString(argOIs));
        }
        xOI = HiveUtils.asListOI(argOIs[0]);
        if (!HiveUtils.isNumberOI(xOI.getListElementObjectInspector())) {
            throw new UDFArgumentTypeException(0,
                "Unexpected Object inspector for array<double>: " + argOIs[0]);
        } else {
            xContentOI = (DoubleObjectInspector) xOI.getListElementObjectInspector();
        }

        String optionValues = "";
        if (argOIs.length > 1) {
            optionValues = HiveUtils.getConstString(argOIs[1]);
        }
        processOptions(optionValues);

        firstCall = true;//most variables are initialized at first input (see evaluate(), init()) because dimensions is still unknown at initialization

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("anomaly_score");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        fieldNames.add("anomaly_decision");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);
        fieldNames.add("changepoint_score");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        fieldNames.add("changepoint_decision");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        x = new ArrayRealVector(HiveUtils.asDoubleArray(args[0].get(), xOI, xContentOI));
        if (callCount == 0) {
            dimensions = x.getDimension();
            xHistory = new LinkedList<RealVector>();
            xMeanEstimate = new ArrayRealVector(dimensions);
            xCovar0 = new DiagonalMatrix(dimensions);
            xCovar = new RealMatrix[xRunningWindowSize];
            xModelMatrix = new RealMatrix[xRunningWindowSize];
            xModelCovar = new BlockRealMatrix(dimensions, dimensions);
            for (int i = 0; i < dimensions; i++) {
                xModelCovar.addToEntry(i, i, 1.d);
            }
            yRunningSum = 0.d;
            xScoreHistory = new LinkedList<Double>();
            yMeanEstimate = 0.d;
            yCovar = new double[yRunningWindowSize + 1];
            yModelCoeff = new double[yRunningWindowSize];
            yModelVar = 1.d;
            for (int i = 0; i < xRunningWindowSize; i++) {
                xHistory.add(new ArrayRealVector(dimensions));
                xCovar[i] = new BlockRealMatrix(dimensions, dimensions);
                xModelMatrix[i] = new BlockRealMatrix(dimensions, dimensions);
            }
            for (int i = 0; i < yRunningWindowSize; i++) {
                xScoreHistory.add(new Double(0.d));
            }
            xTrain();
            double xScore = calcScore(x, xMeanEstimate, xModelCovar);
            yTrain(xScore);
        } else if (dimensions != x.getDimension()) {
            throw new HiveException("Input vector dimension mismatch: " + x.getDimension()
                    + " vs. expected dim: " + dimensions);
        }

        double xScore = Math.min(xThreshold * 100.d, calcScore(x, xMeanEstimate, xModelCovar));
        xTrain();
        double yScore = calcScore(y, yMeanEstimate, yModelVar);
        yTrain(xScore);

        firstCall = false;
        Object[] output = new Object[4];
        output[0] = new DoubleWritable(xScore);
        output[1] = new BooleanWritable(xScore >= xThreshold);
        output[2] = new DoubleWritable(yScore);
        output[3] = new BooleanWritable(yScore >= yThreshold);
        return output;
    }

    private void xTrain() {
        //mean vector
        xMeanEstimate = xMeanEstimate.mapMultiplyToSelf((1.d - xForgetfulness))
                                     .add(x.mapMultiply(xForgetfulness));
        //residuals
        RealVector xResidual0 = x.copy().subtract(xMeanEstimate);

        RealVector[] xResiduals = new RealVector[xRunningWindowSize];

        for (int i = 0; i < xRunningWindowSize; i++) {
            xResiduals[i] = xHistory.get(xRunningWindowSize - i - 1).subtract(xMeanEstimate);
        }

        //covariance matrices
        double[] xCovarA = xCovar0.getDataRef();
        double[] xCovarB = xResidual0.ebeMultiply(xResidual0).mapMultiply(xForgetfulness).toArray();
        for (int i = 0; i < dimensions; i++) {
            xCovarA[i] = xCovarA[i] * (1.d - xForgetfulness) + xCovarB[i];
        }
        xCovar0 = new DiagonalMatrix(xCovarA);
        for (int i = 0; i < xRunningWindowSize; i++) {
            xCovar[i] = xCovar[i].scalarMultiply(1.d - xForgetfulness).add(
                xResidual0.mapMultiply(xForgetfulness).outerProduct(xResiduals[i]));
        }
        //model matrices
        for (int i = 0; i < xRunningWindowSize; i++) {
            RealMatrix C = xCovar[i];
            for (int j = 0; j < i; j++) {
                C = C.subtract(xModelMatrix[j].multiply(xCovar[i - j - 1]));
            }
            xModelMatrix[i] = divide(C, xCovar0);
        }
        //x estimate
        xEstimate = xMeanEstimate.copy();
        for (int i = 0; i < xRunningWindowSize; i++) {
            xEstimate = xEstimate.add(xModelMatrix[i].operate(xResiduals[i]));
        }
        //sigma
        RealVector xEstimateResidual = x.subtract(xEstimate);
        xModelCovar = xModelCovar.scalarMultiply(1.d - xForgetfulness).add(
            xEstimateResidual.mapMultiply(xForgetfulness).outerProduct(xEstimateResidual));

        xHistory.removeLast();
        xHistory.addFirst(x);

        return;
    }

    private void yTrain(double xScore) {
        yRunningSum += xScore - xScoreHistory.getFirst();
        xScoreHistory.add(xScore);
        y = yRunningSum / yRunningWindowSize;
        //mean vector
        yMeanEstimate = yMeanEstimate * (1.d - yForgetfulness) + y * yForgetfulness;
        //residuals
        double[] yResiduals = new double[yRunningWindowSize + 1];
        Iterator<Double> scoresNewToOld = xScoreHistory.descendingIterator();
        for (int i = 0; scoresNewToOld.hasNext(); i++) {
            yResiduals[i] = scoresNewToOld.next() - yMeanEstimate;
        }
        //variance
        double yRes0 = yResiduals[0];
        for (int i = 0; i <= yRunningWindowSize; i++) {
            yCovar[i] = yCovar[i] * (1.d - yForgetfulness) + yRes0 * yResiduals[i] * yForgetfulness;
        }
        //model coefficients
        for (int i = 0; i < yRunningWindowSize; i++) {
            double C = yCovar[i + 1];
            for (int j = 0; j < i; j++) {
                C -= yModelCoeff[j] * yCovar[i - j];
            }
            yModelCoeff[i] = C / yCovar[0];
        }
        //y estimate
        yEstimate = yMeanEstimate;
        for (int i = 0; i < yRunningWindowSize; i++) {
            yEstimate += yModelCoeff[i] * yResiduals[i + 1];
        }
        //sigma
        double yEstimateResidual = y - yEstimate;
        yModelVar = yModelVar * (1.d - yForgetfulness)
                + (yEstimateResidual * yEstimateResidual * yForgetfulness);

        xScoreHistory.removeFirst();

        return;
    }

    private double calcScore(double y, double mean, double var) {
        return -Math.log(
            Math.pow(new NormalDistribution(mean, Math.sqrt(var)).density(y), 1.d / dimensions));
    }

    private double calcScore(RealVector x, RealVector means, RealMatrix covar) {
        return -Math.log(Math.pow(
            new MultivariateNormalDistribution(null, means.toArray(), covar.getData()).density(
                x.toArray()),
            1.d / dimensions));
    }

    //package-private getters for ChangeFinderUDFTest
    final int getxRunningWindowSize() {
        return xRunningWindowSize;
    }

    final double getxForgetfulness() {
        return xForgetfulness;
    }

    final double getxThreshold() {
        return xThreshold;
    }

    final RealVector getxEstimate() {
        return xEstimate;
    }

    final RealMatrix getxModelCovar() {
        return xModelCovar;
    }

    final int getyRunningWindowSize() {
        return yRunningWindowSize;
    }

    final double getyForgetfulness() {
        return yForgetfulness;
    }

    final double getyThreshold() {
        return yThreshold;
    }

    final double getyEstimate() {
        return yEstimate;
    }

    final double getyModelVar() {
        return yModelVar;
    }

}
