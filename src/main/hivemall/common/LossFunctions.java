/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013
 *   National Institute of Advanced Industrial Science and Technology (AIST)
 *   Registration Number: H25PRO-1520
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package hivemall.common;

/**
 * @link https://github.com/JohnLangford/vowpal_wabbit/wiki/Loss-functions
 */
public final class LossFunctions {

    public enum LossType {
        SquaredLoss, LogLoss, HingeLoss, SquaredHingeLoss, QuantileLoss, EpsilonInsensitiveLoss
    }

    public static LossFunction getLossFunction(String type) {
        if("SquaredLoss".equalsIgnoreCase(type)) {
            return new SquaredLoss();
        } else if("LogLoss".equalsIgnoreCase(type)) {
            return new LogLoss();
        } else if("HingeLoss".equalsIgnoreCase(type)) {
            return new HingeLoss();
        } else if("SquaredHingeLoss".equalsIgnoreCase(type)) {
            return new SquaredHingeLoss();
        } else if("QuantileLoss".equalsIgnoreCase(type)) {
            return new QuantileLoss();
        } else if("EpsilonInsensitiveLoss".equalsIgnoreCase(type)) {
            return new EpsilonInsensitiveLoss();
        }
        throw new IllegalArgumentException("Unsupported type: " + type);
    }

    public static LossFunction getLossFunction(LossType type) {
        switch(type) {
            case SquaredLoss:
                return new SquaredLoss();
            case LogLoss:
                return new LogLoss();
            case HingeLoss:
                return new HingeLoss();
            case SquaredHingeLoss:
                return new SquaredHingeLoss();
            case QuantileLoss:
                return new QuantileLoss();
            case EpsilonInsensitiveLoss:
                return new EpsilonInsensitiveLoss();
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    public interface LossFunction {

        /**
         * Evaluate the loss function. 
         * 
         * @param p The prediction, p = w^T x
         * @param y The true value (aka target)
         * @return The loss evaluated at `p` and `y`.
         */
        public float loss(float p, float y);

        /**
         * Evaluate the derivative of the loss function with respect to the prediction `p`.
         * 
         * @param p The prediction, p = w^T x
         * @param y The true value (aka target)
         * @return The derivative of the loss function w.r.t. `p`.
         */
        public float dloss(float p, float y);

        public boolean forBinaryClassification();

        public boolean forRegression();

    }

    public static abstract class BinaryLoss implements LossFunction {

        protected static void checkTarget(float y) {
            if(!(y == 1.f || y == -1.f)) {
                throw new IllegalArgumentException("target must be [+1,-1]: " + y);
            }
        }

        @Override
        public boolean forBinaryClassification() {
            return true;
        }

        @Override
        public boolean forRegression() {
            return false;
        }
    }

    public static abstract class RegressionLoss implements LossFunction {

        @Override
        public boolean forBinaryClassification() {
            return false;
        }

        @Override
        public boolean forRegression() {
            return true;
        }

    }

    /**
     * Squared loss for regression problems.
     * 
     * If you're trying to minimize the mean error, use squared-loss.
     */
    public static final class SquaredLoss extends RegressionLoss {

        @Override
        public float loss(float p, float y) {
            return ((p - y) * (p - y)) / 2.f;
        }

        @Override
        public float dloss(float p, float y) {
            return p - y; // 2 (p - y) / 2
        }
    }

    /**
     * Logistic regression loss for binary classification with y in {-1, 1}.
     */
    public static final class LogLoss extends BinaryLoss {

        /**
         * <code>logloss(p,y) = log(1+exp(-p*y))</code>
         */
        @Override
        public float loss(float p, float y) {
            checkTarget(y);

            float z = y * p;
            if(z > 18.f) {
                return (float) Math.exp(-z);
            }
            if(z < -18.f) {
                return -z;
            }
            return (float) Math.log(1.d + Math.exp(-z));
        }

        @Override
        public float dloss(float p, float y) {
            checkTarget(y);

            float z = y * p;
            if(z > 18.f) {
                return (float) Math.exp(-z) * -y;
            }
            if(z < -18.f) {
                return -y;
            }
            return -y / ((float) Math.exp(z) + 1.f);
        }
    }

    /**
     * Hinge loss for binary classification tasks with y in {-1,1}.     
     */
    public static final class HingeLoss extends BinaryLoss {

        private float threshold;

        public HingeLoss() {
            this(1.f);
        }

        /**         
         * @param threshold Margin threshold. 
         *  When threshold=1.0, one gets the loss used by SVM. 
         *  When threshold=0.0, one gets the loss used by the Perceptron.
         */
        public HingeLoss(float threshold) {
            this.threshold = threshold;
        }

        public void setThreshold(float threshold) {
            this.threshold = threshold;
        }

        @Override
        public float loss(float p, float y) {
            float loss = hingeLoss(p, y, threshold);
            return (loss > 0.f) ? loss : 0.f;
        }

        @Override
        public float dloss(float p, float y) {
            float loss = hingeLoss(p, y, threshold);
            return (loss > 0.f) ? -y : 0.f;
        }
    }

    /**
     * Squared Hinge loss for binary classification tasks with y in {-1,1}.
     */
    public static final class SquaredHingeLoss extends BinaryLoss {

        @Override
        public float loss(float p, float y) {
            return squaredHingeLoss(p, y);
        }

        @Override
        public float dloss(float p, float y) {
            checkTarget(y);

            float d = 1 - (y * p);
            return (d > 0.f) ? -2.f * d * y : 0.f;
        }

    }

    /** 
     * Quantile loss is useful to predict rank/order and you do not mind the mean error to increase 
     * as long as you get the relative order correct. 
     * 
     * @link http://en.wikipedia.org/wiki/Quantile_regression
     */
    public static final class QuantileLoss extends RegressionLoss {

        private float tau;

        public QuantileLoss() {
            this.tau = 0.5f;
        }

        public QuantileLoss(float tau) {
            setTau(tau);
        }

        public void setTau(float tau) {
            if(tau <= 0 || tau >= 1.0) {
                throw new IllegalArgumentException("tau must be in range (0, 1): " + tau);
            }
            this.tau = tau;
        }

        @Override
        public float loss(float p, float y) {
            float e = y - p;
            if(e > 0.f) {
                return tau * e;
            } else {
                return -(1.f - tau) * e;
            }
        }

        @Override
        public float dloss(float p, float y) {
            float e = y - p;
            if(e == 0.f) {
                return 0.f;
            }
            return (e > 0.f) ? -tau : (1.f - tau);
        }

    }

    /**
     * Epsilon-Insensitive loss used by Support Vector Regression (SVR). 
     * <code>loss = max(0, |y - p| - epsilon)</code>
     */
    public static final class EpsilonInsensitiveLoss extends RegressionLoss {

        private float epsilon;

        public EpsilonInsensitiveLoss() {
            this(0.1f);
        }

        public EpsilonInsensitiveLoss(float epsilon) {
            this.epsilon = epsilon;
        }

        public void setEpsilon(float epsilon) {
            this.epsilon = epsilon;
        }

        @Override
        public float loss(float p, float y) {
            float loss = Math.abs(y - p) - epsilon;
            return (loss > 0.f) ? loss : 0.f;
        }

        @Override
        public float dloss(float p, float y) {
            if((y - p) > epsilon) {// real value > predicted value - epsilon
                return -1.f;
            }
            if((p - y) > epsilon) {// real value < predicted value - epsilon
                return 1.f;
            }
            return 0.f;
        }

    }

    public static float hingeLoss(final float p, final float y, final float threshold) {
        BinaryLoss.checkTarget(y);

        float z = y * p;
        return threshold - z;
    }

    public static float hingeLoss(float p, float y) {
        return hingeLoss(p, y, 1.f);
    }

    public static float squaredHingeLoss(final float p, final float y) {
        BinaryLoss.checkTarget(y);

        float z = y * p;
        float d = 1.f - z;
        return (d > 0.f) ? (d * d) : 0.f;
    }

    /**
     * Math.abs(target - predicted) - epsilon
     */
    public static float epsilonInsensitiveLoss(float predicted, float target, float epsilon) {
        return Math.abs(target - predicted) - epsilon;
    }
}
