/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013-2014
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
package hivemall.mf;

import hivemall.io.Rating;
import hivemall.io.Rating.RatingWithSquaredGrad;
import hivemall.utils.lang.Primitives;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

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
        opts.addOption("scale", true, "Internal scaling/descaling factor for cumulative weights [100]");
        return opts;
    }

    @Override
    public Rating newRating(float v) {
        return new RatingWithSquaredGrad(v);
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        CommandLine cl = super.processOptions(argOIs);
        if(cl == null) {
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
    protected float eta() {
        return eta; // dummy
    }

    @Override
    protected void updateItemRating(Rating rating, float Pu, float Qi, double err, float eta) {
        double gradient = err * Pu - lambda * Qi;
        updateRating(rating, Qi, gradient);
    }

    @Override
    protected void updateUserRating(Rating rating, float Pu, float Qi, double err, float eta) {
        double gradient = err * Qi - lambda * Pu;
        updateRating(rating, Pu, gradient);
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
        double gradBu = err - lambda * Bu;
        updateRating(ratingBu, Bu, gradBu);

        Rating ratingBi = model.itemBias(item);
        float Bi = ratingBi.getWeight();
        double gradBi = err - lambda * Bi;
        updateRating(ratingBi, Bi, gradBi);
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
