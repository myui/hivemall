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
import smile.math.matrix.CholeskyDecomposition;
import smile.stat.distribution.MultivariateGaussianDistribution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;

import javax.annotation.Nonnull;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.BlockRealMatrix;
import org.apache.commons.math3.linear.DefaultRealMatrixPreservingVisitor;
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

@Description(
        name = "cf_detect",
        value = "_FUNC_(array<double> x [, const string options]) - Returns anomaly/change-point scores and decisions via ChangeFinder")
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
    private RealMatrix xCovar0;
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
    private LinkedList<Double> xScoreHistory;//newest to oldest
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
            throw new UDFArgumentTypeException(0, "Unexpected Object inspector for array<double>: "
                    + argOIs[0]);
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

    private void init(RealVector x) {
        dimensions = x.getDimension();
        xHistory = new LinkedList<RealVector>();
        xMeanEstimate = new ArrayRealVector(dimensions);
        xCovar0 = new BlockRealMatrix(dimensions, dimensions);
        xCovar = new RealMatrix[xRunningWindowSize];
        xModelMatrix = new RealMatrix[xRunningWindowSize];
        xModelCovar = new BlockRealMatrix(dimensions, dimensions);

        UncorrelatedRandomVectorGenerator gen = new UncorrelatedRandomVectorGenerator(dimensions,
            new UniformRandomGenerator(new JDKRandomGenerator()));
        for (int i = 0; i < xRunningWindowSize; i++) {
            ArrayRealVector rand = new ArrayRealVector(gen.nextVector()).add(x);
            xHistory.addFirst(rand);
            xMeanEstimate = xMeanEstimate.add(rand.mapMultiply(xForgetfulness));
            xMeanEstimate.mapMultiplyToSelf(1.d - xForgetfulness);
        }
        {
            RealVector xResidual0 = x.copy().subtract(xMeanEstimate);
            xCovar0 = xResidual0.outerProduct(xResidual0);
            int i = 0;
            for (Iterator<RealVector> xNewToOld = xHistory.iterator(); xNewToOld.hasNext(); i++) {
                xCovar[i] = xResidual0.outerProduct(xNewToOld.next().subtract(xMeanEstimate));
            }
        }
        yRunningSum = 0.d;
        xScoreHistory = new LinkedList<Double>();
        yCovar = new double[yRunningWindowSize + 1];
        yModelCoeff = new double[yRunningWindowSize];
        yModelVar = 1.d;
        Random rand = new Random();
        for (int i = 0; i < yRunningWindowSize; i++) {
            double nextScore = xThreshold / 2.d + rand.nextDouble() - 0.5d;
            xScoreHistory.addFirst(new Double(nextScore));
            yRunningSum += nextScore;
        }
        yMeanEstimate = yRunningSum / yRunningWindowSize;
        {
            double lastScore = xThreshold / 2.d + rand.nextDouble() - 0.5d;
            yRunningSum += lastScore - xScoreHistory.getLast();
            yMeanEstimate = yMeanEstimate * (1.d - yForgetfulness) + lastScore * yForgetfulness;
            xScoreHistory.addFirst(lastScore);
            for (int i = 0; i <= yRunningWindowSize; i++) {
                yCovar[i] = lastScore * xScoreHistory.get(i);
            }
            xScoreHistory.removeLast();
        }

        return;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        x = new ArrayRealVector(HiveUtils.asDoubleArray(args[0].get(), xOI, xContentOI));
        double xScore, yScore;
        if (firstCall) {
            init(x);
            xTrain();
            xScore = Math.min(xThreshold * 100.d, calcScore(x, xMeanEstimate, xModelCovar));
            yTrain(xScore);
            yScore = Math.min(yThreshold * 100.d, calcScore(y, yMeanEstimate, yModelVar));
        } else {
            if (dimensions != x.getDimension()) {
                throw new HiveException("Input vector dimension mismatch: " + x.getDimension()
                + " vs. expected dim: " + dimensions);
            }
            xScore = Math.min(xThreshold * 100.d, calcScore(x, xMeanEstimate, xModelCovar));
            xTrain();
            yScore = Math.min(yThreshold * 100.d, calcScore(y, yMeanEstimate, yModelVar));
            yTrain(xScore);
        }

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
            xResiduals[i] = xHistory.get(i).subtract(xMeanEstimate);
        }

        //covariance matrices
        xCovar0 = xCovar0.scalarMultiply(1.d - xForgetfulness).add(xResidual0.mapMultiply(xForgetfulness).outerProduct(xResidual0));
        for (int i = 0; i < xRunningWindowSize; i++) {
            xCovar[i] = xCovar[i].scalarMultiply(1.d - xForgetfulness).add(xResidual0.mapMultiply(xForgetfulness).outerProduct(xResiduals[i]));
        }
        //model matrices
        RealMatrix[][] xCovarsCombinedRaw = new RealMatrix[xRunningWindowSize][xRunningWindowSize];
        for (int i = 0; i < xRunningWindowSize; i++) {
            xCovarsCombinedRaw[i][i] = xCovar0;
            for (int j = 0; j < i; j++) {
                int index = i-j-1;
                xCovarsCombinedRaw[i][j] = xCovar[index].transpose();
            }
        }
        RealMatrix[][] xCovarVector = new RealMatrix[xRunningWindowSize][1];
        for (int i = 0; i < xRunningWindowSize; i++) {
            xCovarVector[i][0] = xCovar[i];
        }
        CholeskyDecomposition xCholesky = new CholeskyDecomposition(combineMatricesRaw(xCovarsCombinedRaw));
        RealMatrix invert = MatrixUtils.inverse(combineMatrices(xCovarsCombinedRaw));//FIXME remove later (debug)
        RealMatrix inverse = new BlockRealMatrix(xCholesky.inverse());//FIXME unnecessary
        RealMatrix diff = inverse.subtract(invert);//FIXME remove later (debug)
        double[][] xCovarVectorsCombined = combineMatricesRaw(xCovarVector);
        double[][] xModelMatricesCombinedRaw = new double[xCovarVectorsCombined.length][xCovarVectorsCombined[0].length];
        xCholesky.solve(xCovarVectorsCombined, xModelMatricesCombinedRaw);
        RealMatrix xModelMatricesCombined = new BlockRealMatrix(BlockRealMatrix.toBlocksLayout(xModelMatricesCombinedRaw));
        
        //x estimate
        xEstimate = new ArrayRealVector(dimensions);
        for (int i = 0; i < xRunningWindowSize; i++) {
            int offset = i*dimensions;
            xModelMatrix[i] = new BlockRealMatrix(dimensions, dimensions);
            xModelMatrix[i] = xModelMatricesCombined.getSubMatrix(0, dimensions-1, offset, offset + dimensions-1);
            xEstimate = xEstimate.add(xModelMatrix[i].operate(xResiduals[i]));
        }
        //sigma
        RealVector xEstimateResidual = xResidual0.subtract(xEstimate);//(x - mu) - (xhat - mu) = x - xhat
        xModelCovar = xModelCovar.scalarMultiply(1.d - xForgetfulness).add(
            xEstimateResidual.mapMultiply(xForgetfulness).outerProduct(xEstimateResidual));

        if (firstCall) {
            firstCall = false;
        } else {
            xHistory.removeLast();
            xHistory.addFirst(x);
        }
        
        return;
    }

    private void yTrain(double xScore) {
        yRunningSum += xScore - xScoreHistory.getLast();
        xScoreHistory.addFirst(xScore);
        y = yRunningSum / yRunningWindowSize;
        //mean vector
        yMeanEstimate = yMeanEstimate * (1.d - yForgetfulness) + y * yForgetfulness;
        //residuals
        double[] yResiduals = new double[yRunningWindowSize + 1];
        {
            int i = 0;
            for (Iterator<Double> scoresNewToOld = xScoreHistory.iterator(); scoresNewToOld.hasNext(); i++) {
                yResiduals[i] = scoresNewToOld.next() - yMeanEstimate;
            }
        }
        //variance
        double yRes0 = yResiduals[0];
        for (int i = 0; i <= yRunningWindowSize; i++) {
            yCovar[i] = yCovar[i] * (1.d - yForgetfulness) + yRes0 * yResiduals[i] * yForgetfulness;
        }
        //model coefficients
        double[] yCovarVectorRaw = new double[yRunningWindowSize];
        double[][] yCovarsCombinedRaw = new double[yRunningWindowSize][yRunningWindowSize];
        for (int i = 0; i < yRunningWindowSize; i++) {
            yCovarVectorRaw[i] = yCovar[i+1];
            yCovarsCombinedRaw[i][i] = yCovar[0];
            for (int j = 0; j < i; j++) {
                yCovarsCombinedRaw[i][j] = yCovar[i-j];
                yCovarsCombinedRaw[j][i] = yCovar[i-j];
            }
        }
        

        RealMatrix[][] xCovarVector = new RealMatrix[1][xRunningWindowSize];
        for (int i = 0; i < xRunningWindowSize; i++) {
            xCovarVector[0][i] = xCovar[i];
        }
        CholeskyDecomposition yCholesky = new CholeskyDecomposition(yCovarsCombinedRaw);
        
        RealVector yCovarVector = new ArrayRealVector(yCovarVectorRaw);
        yCholesky.solve(yCovarVector.toArray(), yModelCoeff);
        
        RealMatrix invert = MatrixUtils.inverse(new BlockRealMatrix(yCovarsCombinedRaw));//FIXME remove later (debug)
        RealMatrix inverse = new BlockRealMatrix(yCholesky.inverse());//FIXME unnecessary
        RealMatrix diff = inverse.subtract(invert);//FIXME remove later (debug)
        
        
        
        //y estimate
        yEstimate = yMeanEstimate;
        for (int i = 0; i < yRunningWindowSize; i++) {
            yEstimate += yModelCoeff[i] * yResiduals[i + 1];
        }
        //sigma
        double yEstimateResidual = y - yEstimate;
        yModelVar = yModelVar * (1.d - yForgetfulness)
                + (yEstimateResidual * yEstimateResidual * yForgetfulness);

        xScoreHistory.removeLast();

        return;
    }

    private double calcScore(double y, double mean, double var) {
        return -Math.log(Math.pow(new NormalDistribution(mean, Math.sqrt(var)).density(y),
            1.d / dimensions));
    }

    private double calcScore(RealVector x, RealVector means, RealMatrix covar) {
        MultivariateGaussianDistribution dist = new MultivariateGaussianDistribution(
            means.toArray(), covar.getData());
        double pdf = dist.p(x.toArray());
        return -Math.log(Math.pow(pdf, 1.d / dimensions));
    }

    private RealMatrix combineMatrices(RealMatrix[][] grid) {//combine grid of dim*dim matrices into one
            RealMatrix combined = new BlockRealMatrix(grid.length*dimensions, grid[0].length*dimensions);
            for (int i = 0; i < grid.length; i++) {
                for (int j = 0; j < grid[i].length; j++) {
                    if (j > i) {
                        break;
                    }
                    combined.setSubMatrix(grid[i][j].getData(), i*dimensions, j*dimensions);
                }
            }
            return combined;
        }
    
    private double[][] combineMatricesRaw(RealMatrix[][] grid) {//combine grid of dim*dim matrices into one
        CombineMatrixVisitor v = new CombineMatrixVisitor(grid.length*dimensions, grid[0].length*dimensions, dimensions);
        for (int i = 0; i < grid.length; i++) {
            for (int j = 0; j < grid[i].length; j++) {
                if (j > i) {
                    break;
                }
                v.setNextPosition(i, j);
                grid[i][j].walkInOptimizedOrder(v);
            }
        }
        return v.getWalkResult();
    }
    
    private static class CombineMatrixVisitor extends DefaultRealMatrixPreservingVisitor {
        double[][] copy;
        int dimensions;
        int iOffset;
        int jOffset;
        
        public CombineMatrixVisitor(int rows, int columns, int dimensions) {
            this.dimensions = dimensions;
            copy = new double[rows][columns];
        }
        
        public void setNextPosition(int i, int j) {
            iOffset = i*dimensions;
            jOffset = j*dimensions;
        }

        @Override
        public void visit(int row, int column, double value) {
            copy[iOffset + row][jOffset + column] = value;
            return;
        }

        public double[][] getWalkResult() {
            return copy;
        }
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
