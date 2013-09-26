/**
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

    }

    public static final class SquaredLoss implements LossFunction {

        @Override
        public float loss(float p, float y) {
            return ((p - y) * (p - y)) / 2f;
        }

        @Override
        public float dloss(float p, float y) {
            return p - y; // 2 (p - y) / 2
        }
    }

    /**
     * Logistic regression loss for binary classification with y in {-1, 1}.
     */
    public static final class LogLoss implements LossFunction {

        @Override
        public float loss(float p, float y) {
            assert (y == -1f || y == 1f) : y;
            float z = y * p;

            return (float) Math.log(1.d + Math.exp(-z));
        }

        @Override
        public float dloss(float p, float y) {
            assert (y == -1f || y == 1f) : y;
            double z = y * p;

            if(z < 0.d) {
                return (float) (-1.d / (1.d + Math.exp(z)));
            } else {
                double t = Math.exp(-z);
                return (float) (-t / (t + 1.d));
            }
        }

    }

    /**
     * Hinge loss for binary classification tasks with y in {-1,1}.     
     */
    public static final class HingeLoss implements LossFunction {

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
            return (loss > 0.f) ? -1.f : 0.f;
        }

        public static float hingeLoss(float p, float y) {
            return hingeLoss(p, y, 1.f);
        }

        public static float hingeLoss(float p, float y, float threshold) {
            assert (y == -1f || y == 1f) : y;
            float z = y * p;
            return threshold - z;
        }
    }

    /**
     * Squared Hinge loss for binary classification tasks with y in {-1,1}.
     */
    public static final class SquaredHingeLoss implements LossFunction {

        @Override
        public float loss(float p, float y) {
            assert (y == -1f || y == 1f) : y;
            float z = y * p;

            float loss = (1.f - z) * (1.f - z);
            return (loss > 0.f) ? loss : 0.f;
        }

        @Override
        public float dloss(float p, float y) {
            assert (y == -1f || y == 1f) : y;
            float z = y * p;

            float loss = (1.f - z) * (1.f - z);
            return (loss > 0.f) ? -2 * (1.f - z) : 0.f;
        }

    }

    public static final class QuantileLoss implements LossFunction {

        private float tau;

        public QuantileLoss() {
            this(0.5f);
        }

        public QuantileLoss(float tau) {
            this.tau = tau;
        }

        public void setTau(float tau) {
            this.tau = tau;
        }

        @Override
        public float loss(float p, float y) {
            if(y > p) {
                return tau * (y - p);
            } else {
                return (1.f - tau) * (p - y);
            }
        }

        @Override
        public float dloss(float p, float y) {
            return (y > p) ? -tau : (1.f - tau);
        }

    }

    /**
     * Epsilon-Insensitive loss used by SVR. loss = max(0, |y - p| - epsilon).
     */
    public static final class EpsilonInsensitiveLoss implements LossFunction {

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
            if(y - p > epsilon) {
                return -1.f;
            }
            if(p - y > epsilon) {
                return 1.f;
            }
            return 0.f;
        }

        /**
         * Math.abs(target - predicted) - epsilon
         */
        public static float loss(float predicted, float target, float epsilon) {
            return Math.abs(target - predicted) - epsilon;
        }

    }
}
