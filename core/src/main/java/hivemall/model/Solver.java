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
package hivemall.model;

import javax.annotation.Nonnull;
import java.util.Map;

public interface Solver {

    public enum SolverType {
        Default, Averaging, AdaDelta, AdaGrad, Adam,
    }

    float computeUpdatedValue(@Nonnull Object feature, float weight, float xi, float gradient);

    // Count up #step to tune learning rate
    void proceedStep();

    static abstract class SolverBase implements Solver {

        protected int numStep = 0;

        @Override
        public void proceedStep() {
            numStep++;
        }

    }

    static abstract class AdaGradSolver extends SolverBase {

        private final float eta;
        private final float eps;
        private final float scaling;

        public AdaGradSolver(Map<String, String> solverOptions) {
            float eta = 1.0f;
            float eps = 1.0f;
            float scaling = 100.0f;
            if (solverOptions.containsKey("eta")) {
                eta = Float.parseFloat(solverOptions.get("eta"));
            }
            if (solverOptions.containsKey("eps")) {
                eta = Float.parseFloat(solverOptions.get("eps"));
            }
            if (solverOptions.containsKey("scaling")) {
                eta = Float.parseFloat(solverOptions.get("scaling"));
            }
            this.eta = eta;
            this.eps = eps;
            this.scaling = scaling;
        }

        // Directly update a given `weight` in terms of performance
        protected void computeUpdateValue(@Nonnull IWeightValue weight, float xi, float gradient) {
            float scaled_sum_sqgrad = weight.getSumOfSquaredGradients() + gradient * (gradient / scaling);
            float coeff = eta(scaled_sum_sqgrad * scaling) * gradient;
            float new_weight = weight.get() + coeff * xi;
            weight.set(new_weight);
            weight.setSumOfSquaredGradients(scaled_sum_sqgrad);
        }

        private float eta(double sumOfSquaredGradients) {
            // Always less than `eta`
            return (float) (this.eta / (Math.sqrt(sumOfSquaredGradients) + eps));
        }

    }

    /**
     * AdamSolver, an algorithm for first-order gradient-based optimization of stochastic
     * objective functions, based on adaptive estimates of lower-order moments.
     *
     *  - D. P. Kingma and J. L. Ba: "ADAM: A Method for Stochastic Optimization." arXiv preprint arXiv:1412.6980v8, 2014.
     */
    static abstract class AdamSolver extends SolverBase {

        private final float eta;
        private final float beta;
        private final float gamma;
        private final float eps_hat;

        public AdamSolver(Map<String, String> solverOptions) {
            float eta = 1.0f;
            float beta = 0.9f;
            float gamma = 0.999f;
            float eps_hat = 1e-8f;
            if (solverOptions.containsKey("eta")) {
                eta = Float.parseFloat(solverOptions.get("eta"));
            }
            if (solverOptions.containsKey("beta")) {
                beta = Float.parseFloat(solverOptions.get("beta"));
            }
            if (solverOptions.containsKey("gamma")) {
                gamma = Float.parseFloat(solverOptions.get("gamma"));
            }
            if (solverOptions.containsKey("eps_hat")) {
                eps_hat = Float.parseFloat(solverOptions.get("eps_hat"));
            }
            this.eta = eta;
            this.beta = beta;
            this.gamma = gamma;
            this.eps_hat = eps_hat;
        }

        // Directly update a given `weight` in terms of performance
        protected void computeUpdateValue(@Nonnull IWeightValue weight, float xi, float gradient) {
            float val_m = beta * weight.getM() + (1.f - beta) * gradient;
            float val_v = gamma * weight.getV() + (float) ((1.f - gamma) * Math.pow(gradient, 2.0));
            float val_m_hat = (float) (val_m / (1.f - Math.pow(beta, numStep)));
            float val_v_hat = (float) (val_v / (1.f - Math.pow(gamma, numStep)));
            float coeff = (float) (eta * val_m_hat / (Math.sqrt(val_v_hat) + eps_hat));
            float new_weight = weight.get() + coeff + xi;
            weight.set(new_weight);
            weight.setM(val_m);
            weight.setV(val_m);
        }
    }

}
