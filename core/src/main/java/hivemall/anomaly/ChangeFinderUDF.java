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
import smile.stat.distribution.MultivariateGaussianDistribution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;

import javax.annotation.Nonnull;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.BlockRealMatrix;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
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
        value = "_FUNC_(array<double> x [, const string options]) - Returns anomaly/change-point scores and decisions via ChangeFinder")
public class ChangeFinderUDF extends UDFWithOptions {
    private ListObjectInspector xOI;
    private DoubleObjectInspector xContentOI;

    private int dimensions;
    private long callCount;
    private boolean xHistoryFull;
    private boolean xCovarFull;
    private boolean xModelCovarFull;
    private boolean xScoreHistoryFull;
    private boolean yCovarFull;
    private boolean yModelCovarFull;

    private RealVector x;
    private LinkedList<RealVector> xHistory;//history is ordered from newest to oldest, i.e. xHistory.getFirst() is from t-1.
    private RealVector xMeanEstimate;//\hat{µ}
    private RealMatrix xCovar0;//C_0
    private RealMatrix[] xCovar;//C_j
    private RealMatrix[] xModelMatrix;//A_i
    private RealVector xEstimate;//\hat{x}
    private RealMatrix xModelCovar;//∑
    private int xRunningWindowSize;
    private double xForgetfulness;
    private double xThreshold;

    private double y;
    private double yRunningSum;
    private LinkedList<Double> xScoreHistory;//newest to oldest
    private double yMeanEstimate;
    private double[] yCovar;
    private double[] yModelCoeff;
    private double yEstimate;
    private double yModelVar;
    private int yRunningWindowSize;
    private double yForgetfulness;
    private double yThreshold;
    private LinkedList<Double> yScoreHistory;
    private double yScoreRunningSum;


    @Override
    public String getDisplayString(String[] args) {
        return "cf_detect(" + Arrays.toString(args) + ")";
    }

    @Override
    protected Options getOptions() {
        Options opts = new Options();
        opts.addOption("aWindow", "anomalyDetectionRunningWindowSize", true,
            "Number of past samples to include in anomaly detection calculation [default: 10]");
        opts.addOption("cWindow", "changePointDetectionRunningWindowSize", true,
            "Number of past samples to include in change-point detection calculation [default: 10]");
        opts.addOption("aForget", "anomalyDetectionForgetfulness", true,
            "Forgetfulness parameter for anomaly detection [range: (0,1); default: 0.02]");
        opts.addOption("cForget", "changePointDetectionForgetfulness", true,
            "Forgetfulness parameter for change-point detection [range: (0,1); default: 0.02]");
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

        callCount = 0L;//most variables are initialized at first input (see evaluate(), init()) because dimensions is still unknown at initialization

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

    private void init(RealVector x) {
        dimensions = x.getDimension();
        xHistory = new LinkedList<RealVector>();
        xMeanEstimate = x.copy();
        xCovar0 = new BlockRealMatrix(dimensions, dimensions);
        xCovar = new RealMatrix[xRunningWindowSize];
        xModelMatrix = new RealMatrix[xRunningWindowSize];
        xModelCovar = new BlockRealMatrix(dimensions, dimensions);
        for (int i = 0; i < xRunningWindowSize; i++) {
            xHistory.addFirst(new ArrayRealVector(dimensions));
            xCovar[i] = new BlockRealMatrix(dimensions, dimensions);
        }
        yRunningSum = 0.d;
        xScoreHistory = new LinkedList<Double>();
        yScoreHistory = new LinkedList<Double>();
        yCovar = new double[yRunningWindowSize + 1];
        yModelCoeff = new double[yRunningWindowSize];
        yModelVar = 1.d;
        for (int i = 0; i < yRunningWindowSize; i++) {
            xScoreHistory.addFirst(new Double(0.d));
            yScoreHistory.addFirst(new Double(0.d));
        }
        yScoreRunningSum = 0.d;

        return;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        x = new ArrayRealVector(HiveUtils.asDoubleArray(args[0].get(), xOI, xContentOI));
        double xScore = xThreshold == 0.d ? -1.d : xThreshold / 2.d;
        double yScore = yThreshold == 0.d ? -1.d : yThreshold / 2.d;
        if (callCount == 0L) {
            init(x);
        } else if (dimensions != x.getDimension()) {
            throw new HiveException("Input vector dimension mismatch: " + x.getDimension()
                    + " vs. expected dim: " + dimensions);
        }
        xHistoryFull = callCount >= xRunningWindowSize;
        xCovarFull = callCount >= xRunningWindowSize + dimensions;
        xModelCovarFull = callCount >= xRunningWindowSize + 2 * dimensions;
        xScoreHistoryFull = callCount >= xRunningWindowSize + 2 * dimensions + yRunningWindowSize;
        yCovarFull = callCount >= xRunningWindowSize + 2 * dimensions + yRunningWindowSize + 1;
        yModelCovarFull = callCount >= xRunningWindowSize + 2 * dimensions + yRunningWindowSize + 2;
        if (xModelCovarFull) {
            double temp = calcScore(x, xMeanEstimate, xModelCovar);
            if (Double.isInfinite(temp)) {//Recover from Infinity
                xScore = xThreshold;
            } else if (!(Double.isNaN(temp))) {
                xScore = temp;
            }//Ignore NaN
        }
        xTrain();
        if (xModelCovarFull) {
            yRunningSum += xScore;
            yRunningSum -= xScoreHistory.getLast();
            y = yRunningSum / yRunningWindowSize;
            if (yModelCovarFull) {
                double temp = calcScore(y, yMeanEstimate, yModelVar);
                if (Double.isInfinite(temp)) {//Recover from Infinity
                    yScore = yThreshold;
                } else if (!(Double.isNaN(temp))) {
                    yScore = temp;
                }//Ignore NaN
                yScoreRunningSum += yScore;
                yScoreHistory.addFirst(yScore);
                yScoreRunningSum -= yScoreHistory.getLast();
                yScoreHistory.removeLast();
                yScore = yScoreRunningSum / yRunningWindowSize;
            }
            yTrain(xScore);
        }

        callCount++;
        Object[] output = new Object[4];
        output[0] = new DoubleWritable(xScore);
        output[1] = new BooleanWritable(xScore >= xThreshold);
        output[2] = new DoubleWritable(yScore);
        output[3] = new BooleanWritable(yScore >= yThreshold);
        return output;
    }

    private void xTrain() {
        //update mean vector
        //\hat{µ} := (1-r) \hat{µ} + r x
        xMeanEstimate = xMeanEstimate.mapMultiplyToSelf((1.d - xForgetfulness))
                                     .add(x.mapMultiply(xForgetfulness));

        //residuals (x - \hat{µ})
        RealVector xResidual0 = x.copy().subtract(xMeanEstimate);
        RealVector[] xResiduals = new RealVector[xRunningWindowSize];
        for (int i = 0; i < xRunningWindowSize; i++) {
            xResiduals[i] = xHistory.get(i).subtract(xMeanEstimate);
        }

        //update covariance matrices
        //C_j := (1-r) C_j + r (x_t - \hat{µ}) (x_{t-j} - \hat{µ})'
        xCovar0 = xCovar0.scalarMultiply(1.d - xForgetfulness)
                         .add(xResidual0.mapMultiply(xForgetfulness).outerProduct(xResidual0));
        for (int i = 0; i < (xHistoryFull ? xRunningWindowSize : callCount); i++) {
            xCovar[i] = xCovar[i].scalarMultiply(1.d - xForgetfulness).add(
                xResidual0.mapMultiply(xForgetfulness).outerProduct(xResiduals[i]));
        }

        if (xCovarFull) {
            //solve C_j = ∑_{i=1}^{k} A_i C_{j-i} where j = 1..k, C_{-i} = C'_i, k = xRunningWindowSize for A_i
            //combine RHS covariance matrices into one large symmetric matrix
            /*
             *  xCovarsCombined (symmetric, invertible, k*dimensions by k*dimensions)
             *
             * /C_0     |C_1     |C_2     | .  .  .   |C_{k-1} \
             * |--------+--------+--------+           +--------|
             * |C_1'    |C_0     |C_1     |               .    |
             * |--------+--------+--------+               .    |
             * |C_2'    |C_1'    |C_0     |               .    |
             * |--------+--------+--------+                    |
             * |   .                         .                 |
             * |   .                            .              |
             * |   .                               .           |
             * |--------+                              +-------|
             * \C_{k-1}'| .  .  .                      |C_0    /
             */
            RealMatrix[][] xCovarsCombinedRaw =
                    new RealMatrix[xRunningWindowSize][xRunningWindowSize];
            for (int i = 0; i < xRunningWindowSize; i++) {
                xCovarsCombinedRaw[i][i] = xCovar0;
                for (int j = 0; j < i; j++) {
                    int index = i - j - 1;
                    xCovarsCombinedRaw[i][j] = xCovar[index].transpose();
                    xCovarsCombinedRaw[j][i] = xCovar[index];
                }
            }
            //similarly combine LHS covariances
            /*
             * xCovarVector (k*dimensions by dimensions)
             *
             * /C_1\
             * |---|
             * |C_2|
             * |---|
             * |C_3|
             * | . |
             * | . |
             * | . |
             * \C_n/
             */
            RealMatrix[][] xCovarVector = new RealMatrix[xRunningWindowSize][1];
            for (int i = 0; i < xRunningWindowSize; i++) {
                xCovarVector[i][0] = xCovar[i];
            }
            //solve equation with LU decomp
            RealMatrix combined = combineMatrices(xCovarsCombinedRaw);
            LUDecomposition xLU = new LUDecomposition(combined);
            //solutions (model matrices A_i) are stored in combined format
            /*
             * xModelMatricesCombined (k*dimensions by dimensions)
             *
             * /A_1\
             * |---|
             * |A_2|
             * |---|
             * |A_3|
             * | . |
             * | . |
             * | . |
             * \A_n/
             */
            RealMatrix xModelMatricesCombined =
                    xLU.getSolver().solve(combineMatrices(xCovarVector));

            //x estimate (\hat{x} = \hat{µ} + ∑_{i=1}^k A_i (x_{t-i} - \hat{µ})
            xEstimate = xMeanEstimate.copy();
            for (int i = 0; i < xRunningWindowSize; i++) {
                int offset = i * dimensions;
                xModelMatrix[i] = xModelMatricesCombined.getSubMatrix(offset,
                    offset + dimensions - 1, 0, dimensions - 1);
                xEstimate = xEstimate.add(xModelMatrix[i].operate(xResiduals[i]));
            }

            //update model covariance
            //∑ := (1-r) ∑ + r (x - \hat{x}) (x - \hat{x})'
            RealVector xEstimateResidual = x.subtract(xEstimate);
            xModelCovar = xModelCovar.scalarMultiply(1.d - xForgetfulness).add(
                xEstimateResidual.mapMultiply(xForgetfulness).outerProduct(xEstimateResidual));
        }

        xHistory.removeLast();
        xHistory.addFirst(x);

        return;
    }

    private void yTrain(double xScore) {
        xScoreHistory.addFirst(xScore);

        //update mean vector
        //\hat{µ} := (1-r) \hat{µ} + r y
        yMeanEstimate = yMeanEstimate * (1.d - yForgetfulness) + y * yForgetfulness;

        //residuals (y - \hat{µ})
        double[] yResiduals = new double[yRunningWindowSize + 1];
        {
            int i = 0;
            //Using an Iterator because get(i) for LinkedLists may cause O(n^2) time loop execution.
            for (Iterator<Double> scoresNewToOld =
                    xScoreHistory.iterator(); scoresNewToOld.hasNext(); i++) {
                yResiduals[i] = scoresNewToOld.next() - yMeanEstimate;
            }
        }
        double yRes0 = yResiduals[0];

        //update variance
        //C_j := (1-r) C_j + r (y_t - \hat{µ}) (y_{t-j} - \hat{µ})
        for (int i = 0; i < (xScoreHistoryFull ? yRunningWindowSize
                : callCount - (xRunningWindowSize + 2 * dimensions)); i++) {
            yCovar[i] = yCovar[i] * (1.d - yForgetfulness) + yRes0 * yResiduals[i] * yForgetfulness;
        }

        if (yCovarFull) {
            //solve C_j = ∑_{i=1}^{k} A_i C_{j-i} where j = 1..k, C_{-i} = C'_i, k = yRunningWindowSize for A_i
            //y is always 1D, so A_i and C_j are both scalars.
            //the same format is used, but unlike xTrain(), this is the usual method of solving systems of scalar equations with matrices
            //i.e. each block is not a matrix but a single value
            /*
             *  yVarsCombined                  * yModelCoeff  =   yVarVector
             *                                   ^Solve for this
             * /C_0 |C_1 |C_2 |              \      /A_1\          /C_1\
             * |----+----+----+              |      |---|          |---|
             * |C_1'|C_0 |C_1 |              |      |A_2|          |C_2|
             * |----+----+----+              |      |---|          |---|
             * |C_2'|C_1'|C_0 |              |      |A_3|          |C_3|
             * |----+----+----+              | *    |---|     =    |---|
             * |                .            |      | . |          | . |
             * |                   .         |      | . |          | . |
             * |                      .      |      | . |          | . |
             * |                        +----|      | . |          | . |
             * \                        |C_0 /      \A_k/          \C_k/
             */
            RealVector yVarVector = new ArrayRealVector(yRunningWindowSize);
            RealMatrix yVarsCombined = new BlockRealMatrix(yRunningWindowSize, yRunningWindowSize);
            for (int i = 0; i < yRunningWindowSize; i++) {
                yVarVector.setEntry(i, yCovar[i + 1]);
                yVarsCombined.setEntry(i, i, yCovar[0]);
                for (int j = 0; j < i; j++) {
                    yVarsCombined.setEntry(i, j, yCovar[i - j]);
                    yVarsCombined.setEntry(j, i, yCovar[i - j]);
                }
            }

            //solve with LU decomp
            LUDecomposition yLU = new LUDecomposition(yVarsCombined);
            yModelCoeff = yLU.getSolver().solve(yVarVector).toArray();

            //y estimate (\hat{y} = \hat{µ} + ∑_{i=1}^k A_i (y_{t-i} - \hat{µ})
            yEstimate = yMeanEstimate;
            for (int i = 0; i < yRunningWindowSize; i++) {
                yEstimate += yModelCoeff[i] * yResiduals[i + 1];
            }

            //update model variance
            //∑ := (1-r) ∑ + r (y - \hat{y})^2
            double yEstimateResidual = y - yEstimate;
            yModelVar = yModelVar * (1.d - yForgetfulness)
                    + (yEstimateResidual * yEstimateResidual * yForgetfulness);
        }

        xScoreHistory.removeLast();

        return;
    }

    private double calcScore(double y, double mean, double var) {
        return -Math.log(new NormalDistribution(mean, Math.sqrt(var)).density(y));
    }

    private double calcScore(RealVector x, RealVector means, RealMatrix covar) {
        MultivariateGaussianDistribution dist =
                new MultivariateGaussianDistribution(means.toArray(), covar.getData());
        double pdf = dist.p(x.toArray());
        return -Math.log(Math.pow(pdf, 1.d / dimensions));
    }

    private RealMatrix combineMatrices(RealMatrix[][] grid) {//combine grid of dim*dim matrices into one
        RealMatrix combined =
                new BlockRealMatrix(grid.length * dimensions, grid[0].length * dimensions);
        for (int i = 0; i < grid.length; i++) {
            for (int j = 0; j < grid[i].length; j++) {
                combined.setSubMatrix(grid[i][j].getData(), i * dimensions, j * dimensions);
            }
        }
        return combined;
    }

    //package-private getters for ChangeFinderUDFTest

    final double gety() {
        return y;
    }

    final int getxRunningWindowSize() {
        return xRunningWindowSize;
    }

    final int getyRunningWindowSize() {
        return yRunningWindowSize;
    }

    final double getxForgetfulness() {
        return xForgetfulness;
    }

    final double getyForgetfulness() {
        return yForgetfulness;
    }

    final double getxThreshold() {
        return xThreshold;
    }

    final double getyThreshold() {
        return yThreshold;
    }

    final RealVector getxMeanEstimate() {
        return xMeanEstimate;
    }

    final double getyMeanEstimate() {
        return yMeanEstimate;
    }

    final RealVector getxEstimate() {
        return xEstimate;
    }

    final double getyEstimate() {
        return yEstimate;
    }

    final RealMatrix getxModelCovar() {
        return xModelCovar;
    }

    final double getyModelVar() {
        return yModelVar;
    }

}
