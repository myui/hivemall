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
package hivemall.mf;

import hivemall.mf.Rating.RatingWithSquaredGrad;
import hivemall.utils.lang.Primitives;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

@Description(
        name = "train_mf_adagrad",
        value = "_FUNC_(INT user, INT item, FLOAT rating [, CONSTANT STRING options])"
                + " - Returns a relation consists of <int idx, array<float> Pu, array<float> Qi [, float Bu, float Bi [, float mu]]>")
public final class MatrixFactorizationAdaGradUDTF extends OnlineMatrixFactorizationUDTF {

    private float eta;
    private float eps;
    private float scaling;

    public MatrixFactorizationAdaGradUDTF() {
        super();
    }

    @Override
    protected Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("eta", "eta0", true, "The initial learning rate [default 1.0]");
        opts.addOption("eps", true, "A constant used in the denominator of AdaGrad [default 1.0]");
        opts.addOption("scale", true,
            "Internal scaling/descaling factor for cumulative weights [100]");
        return opts;
    }

    @Override
    public Rating newRating(float v) {
        return new RatingWithSquaredGrad(v);
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        CommandLine cl = super.processOptions(argOIs);
        if (cl == null) {
            this.eta = 1.f;
            this.eps = 1.f;
            this.scaling = 100f;
        } else {
            this.eta = Primitives.parseFloat(cl.getOptionValue("eta"), 1.f);
            this.eps = Primitives.parseFloat(cl.getOptionValue("eps"), 1.f);
            this.scaling = Primitives.parseFloat(cl.getOptionValue("scale"), 100f);
        }
        return cl;
    }

    @Override
    protected void updateItemRating(Rating rating, float Pu, float Qi, double err, float eta) {
        double gradient = err * Pu - lambda * Qi;
        updateRating(rating, Qi, gradient);
        cvState.incrLoss(lambda * Qi * Qi);
    }

    @Override
    protected void updateUserRating(Rating rating, float Pu, float Qi, double err, float eta) {
        double gradient = err * Qi - lambda * Pu;
        updateRating(rating, Pu, gradient);
        cvState.incrLoss(lambda * Pu * Pu);
    }

    @Override
    protected void updateMeanRating(double err, float eta) {
        assert updateMeanRating;
        Rating mean = model.meanRating();
        float oldMean = mean.getWeight();
        updateRating(mean, oldMean, err);
    }

    @Override
    protected void updateBias(int user, int item, double err, float eta) {
        Rating ratingBu = model.userBias(user);
        float Bu = ratingBu.getWeight();
        double Gu = err - lambda * Bu;
        updateRating(ratingBu, Bu, Gu);
        cvState.incrLoss(lambda * Bu * Bu);

        Rating ratingBi = model.itemBias(item);
        float Bi = ratingBi.getWeight();
        double Gi = err - lambda * Bi;
        updateRating(ratingBi, Bi, Gi);
        cvState.incrLoss(lambda * Bi * Bi);
    }

    private void updateRating(final Rating rating, final float oldWeight, final double gradient) {
        double gg = gradient * (gradient / scaling);
        double scaled_sum_gg = rating.getSumOfSquaredGradients() + gg;
        float delta = (float) (eta(scaled_sum_gg) * gradient);
        float newWeight = oldWeight + delta;
        rating.setWeight(newWeight);
        rating.setSumOfSquaredGradients(scaled_sum_gg);
    }

    private float eta(final double scaledSumOfSquaredGradients) {
        double sumOfSquaredGradients = scaledSumOfSquaredGradients * scaling;
        return eta / (float) Math.sqrt(eps + sumOfSquaredGradients); // always less than eta0
    }

}
