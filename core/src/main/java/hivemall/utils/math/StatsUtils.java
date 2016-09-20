/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2016 Makoto YUI
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
package hivemall.utils.math;

import hivemall.utils.lang.Preconditions;

import javax.annotation.Nonnull;

import org.apache.commons.math3.distribution.ChiSquaredDistribution;
import org.apache.commons.math3.linear.DecompositionSolver;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.linear.SingularValueDecomposition;

import java.util.AbstractMap;
import java.util.Map;

public final class StatsUtils {

    private StatsUtils() {}

    /**
     * probit(p)=sqrt(2)erf^-1(2p-1)
     * 
     * <pre>
     * probit(1)=INF, probit(0)=-INF, probit(0.5)=0
     * </pre>
     * 
     * @param p must be in [0,1]
     * @link http://en.wikipedia.org/wiki/Probit
     */
    public static double probit(double p) {
        if (p < 0 || p > 1) {
            throw new IllegalArgumentException("p must be in [0,1]");
        }
        return Math.sqrt(2.d) * MathUtils.inverseErf(2.d * p - 1.d);
    }

    public static double probit(double p, double range) {
        if (range <= 0) {
            throw new IllegalArgumentException("range must be > 0: " + range);
        }
        if (p == 0) {
            return -range;
        }
        if (p == 1) {
            return range;
        }
        double v = probit(p);
        if (v < 0) {
            return Math.max(v, -range);
        } else {
            return Math.min(v, range);
        }
    }

    /**
     * @return value of probabilistic density function
     */
    public static double pdf(final double x, final double x_hat, final double sigma) {
        if (sigma == 0.d) {
            return 0.d;
        }
        double diff = x - x_hat;
        double numerator = Math.exp(-0.5d * diff * diff / sigma);
        double denominator = Math.sqrt(2.d * Math.PI) * Math.sqrt(sigma);
        return numerator / denominator;
    }

    /**
     * pdf(x, x_hat) = exp(-0.5 * (x-x_hat) * inv(Σ) * (x-x_hat)T) / ( 2π^0.5d * det(Σ)^0.5)
     * 
     * @return value of probabilistic density function
     * @link https://en.wikipedia.org/wiki/Multivariate_normal_distribution#Density_function
     */
    public static double pdf(@Nonnull final RealVector x, @Nonnull final RealVector x_hat,
            @Nonnull final RealMatrix sigma) {
        final int dim = x.getDimension();
        Preconditions.checkArgument(x_hat.getDimension() == dim, "|x| != |x_hat|, |x|=" + dim
                + ", |x_hat|=" + x_hat.getDimension());
        Preconditions.checkArgument(sigma.getRowDimension() == dim, "|x| != |sigma|, |x|=" + dim
                + ", |sigma|=" + sigma.getRowDimension());
        Preconditions.checkArgument(sigma.isSquare(), "Sigma is not square matrix");

        LUDecomposition LU = new LUDecomposition(sigma);
        final double detSigma = LU.getDeterminant();
        double denominator = Math.pow(2.d * Math.PI, 0.5d * dim) * Math.pow(detSigma, 0.5d);
        if (denominator == 0.d) { // avoid divide by zero
            return 0.d;
        }

        final RealMatrix invSigma;
        DecompositionSolver solver = LU.getSolver();
        if (solver.isNonSingular() == false) {
            SingularValueDecomposition svd = new SingularValueDecomposition(sigma);
            invSigma = svd.getSolver().getInverse(); // least square solution
        } else {
            invSigma = solver.getInverse();
        }
        //EigenDecomposition eigen = new EigenDecomposition(sigma);
        //double detSigma = eigen.getDeterminant();
        //RealMatrix invSigma = eigen.getSolver().getInverse();

        RealVector diff = x.subtract(x_hat);
        RealVector premultiplied = invSigma.preMultiply(diff);
        double sum = premultiplied.dotProduct(diff);
        double numerator = Math.exp(-0.5d * sum);

        return numerator / denominator;
    }

    public static double logLoss(final double actual, final double predicted, final double sigma) {
        double p = pdf(actual, predicted, sigma);
        if (p == 0.d) {
            return 0.d;
        }
        return -Math.log(p);
    }

    public static double logLoss(@Nonnull final RealVector actual,
            @Nonnull final RealVector predicted, @Nonnull final RealMatrix sigma) {
        double p = pdf(actual, predicted, sigma);
        if (p == 0.d) {
            return 0.d;
        }
        return -Math.log(p);
    }

    /**
     * @param mu1 mean of the first normal distribution
     * @param sigma1 variance of the first normal distribution
     * @param mu2 mean of the second normal distribution
     * @param sigma2 variance of the second normal distribution
     * @return the Hellinger distance between two normal distributions
     * @link https://en.wikipedia.org/wiki/Hellinger_distance#Examples
     */
    public static double hellingerDistance(@Nonnull final double mu1, @Nonnull final double sigma1,
            @Nonnull final double mu2, @Nonnull final double sigma2) {
        double sigmaSum = sigma1 + sigma2;
        if (sigmaSum == 0.d) {
            return 0.d;
        }
        double numerator = Math.pow(sigma1, 0.25d) * Math.pow(sigma2, 0.25d)
                * Math.exp(-0.25d * Math.pow(mu1 - mu2, 2d) / sigmaSum);
        double denominator = Math.sqrt(sigmaSum / 2d);
        if (denominator == 0.d) {
            return 1.d;
        }
        return 1.d - numerator / denominator;
    }

    /**
     * @param mu1 mean vector of the first normal distribution
     * @param sigma1 covariance matrix of the first normal distribution
     * @param mu2 mean vector of the second normal distribution
     * @param sigma2 covariance matrix of the second normal distribution
     * @return the Hellinger distance between two multivariate normal distributions
     * @link https://en.wikipedia.org/wiki/Hellinger_distance#Examples
     */
    public static double hellingerDistance(@Nonnull final RealVector mu1,
            @Nonnull final RealMatrix sigma1, @Nonnull final RealVector mu2,
            @Nonnull final RealMatrix sigma2) {
        RealVector muSub = mu1.subtract(mu2);
        RealMatrix sigmaMean = sigma1.add(sigma2).scalarMultiply(0.5d);
        LUDecomposition LUsigmaMean = new LUDecomposition(sigmaMean);
        double denominator = Math.sqrt(LUsigmaMean.getDeterminant());
        if (denominator == 0.d) {
            return 1.d; // avoid divide by zero
        }
        RealMatrix sigmaMeanInv = LUsigmaMean.getSolver().getInverse(); // has inverse iff det != 0
        double sigma1Det = MatrixUtils.det(sigma1);
        double sigma2Det = MatrixUtils.det(sigma2);

        double numerator = Math.pow(sigma1Det, 0.25d) * Math.pow(sigma2Det, 0.25d)
                * Math.exp(-0.125d * sigmaMeanInv.preMultiply(muSub).dotProduct(muSub));
        return 1.d - numerator / denominator;
    }

    /**
     * @param observed mean vector whose value is observed
     * @param expected mean vector whose value is expected
     * @return chi2 value
     */
    public static double chiSquare(@Nonnull final double[] observed,
            @Nonnull final double[] expected) {
        Preconditions.checkArgument(observed.length == expected.length);

        double sumObserved = 0.d;
        double sumExpected = 0.d;

        for (int ratio = 0; ratio < observed.length; ++ratio) {
            sumObserved += observed[ratio];
            sumExpected += expected[ratio];
        }

        double var15 = 1.d;
        boolean rescale = false;
        if (Math.abs(sumObserved - sumExpected) > 1.e-5) {
            var15 = sumObserved / sumExpected;
            rescale = true;
        }

        double sumSq = 0.d;

        for (int i = 0; i < observed.length; ++i) {
            double dev;
            if (rescale) {
                dev = observed[i] - var15 * expected[i];
                sumSq += dev * dev / (var15 * expected[i]);
            } else {
                dev = observed[i] - expected[i];
                sumSq += dev * dev / expected[i];
            }
        }

        return sumSq;
    }

    /**
     * @param observed means vector whose value is observed
     * @param expected means vector whose value is expected
     * @return p value
     */
    public static double chiSquareTest(@Nonnull final double[] observed,
            @Nonnull final double[] expected) {
        ChiSquaredDistribution distribution = new ChiSquaredDistribution(null,
            (double) expected.length - 1.d);
        return 1.d - distribution.cumulativeProbability(chiSquare(observed, expected));
    }

    /**
     * This method offers effective calculation for multiple entries rather than calculation
     * individually
     * 
     * @param observeds means matrix whose values are observed
     * @param expecteds means matrix
     * @return (chi2 value[], p value[])
     */
    public static Map.Entry<double[], double[]> chiSquares(@Nonnull final double[][] observeds,
            @Nonnull final double[][] expecteds) {
        Preconditions.checkArgument(observeds.length == expecteds.length);

        final int len = expecteds.length;
        final int lenOfEach = expecteds[0].length;

        final ChiSquaredDistribution distribution = new ChiSquaredDistribution(null,
            (double) lenOfEach - 1.d);

        final double[] chi2s = new double[len];
        final double[] ps = new double[len];
        for (int i = 0; i < len; i++) {
            chi2s[i] = chiSquare(observeds[i], expecteds[i]);
            ps[i] = 1.d - distribution.cumulativeProbability(chi2s[i]);
        }

        return new AbstractMap.SimpleEntry<double[], double[]>(chi2s, ps);
    }
}
