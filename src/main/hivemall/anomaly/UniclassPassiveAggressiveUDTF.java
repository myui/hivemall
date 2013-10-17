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
package hivemall.anomaly;

import hivemall.common.LossFunctions;
import hivemall.common.PredictionResult;
import hivemall.common.WeightValue;

import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class UniclassPassiveAggressiveUDTF extends UniclassPredictorUDTF {

    /** Ball of radius */
    protected float epsilon;

    /** Upper bound of radius e */
    protected float B;

    protected boolean learn_radius;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        final int numArgs = argOIs.length;
        if(numArgs != 1 && numArgs != 2) {
            throw new UDFArgumentException(getClass().getSimpleName()
                    + " takes arguments: List<Int|BigInt|Text> features [, constant string options]");
        }

        StructObjectInspector inspector = super.initialize(argOIs);

        this.learn_radius = learnRadius();
        if(learn_radius) {
            weights.put(radiusKey, new WeightValue(B));
        }

        return inspector;
    }

    @Override
    protected Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("e", "radius", true, "Ball of radius (e > 0) [default 1.0]");
        opts.addOption("ub", "upper_bound", true, "Upper bound of radius e (ub > 0) (set ub to learn radius)");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        CommandLine cl = super.processOptions(argOIs);

        float epsilon = 1.f;
        float ub = -1.f;

        if(cl != null) {
            String opt_epsilon = cl.getOptionValue("e");
            if(opt_epsilon != null) {
                epsilon = Float.parseFloat(opt_epsilon);
                if(!(epsilon > 0.f)) {
                    throw new UDFArgumentException("Ball of radius e must be e > 0: " + opt_epsilon);
                }
            }
            String opt_radius = cl.getOptionValue("ub");
            if(opt_radius != null) {
                ub = Float.parseFloat(opt_radius);
                if(!(ub > 0.f)) {
                    throw new UDFArgumentException("Upper bound of radius (ub) must be ub > 0: "
                            + opt_radius);
                }
            }
        }

        this.epsilon = epsilon;
        this.B = ub;
        return cl;
    }

    @Override
    protected float loss(PredictionResult margin) {
        float l2norm = margin.getL2Norm();
        float epsilon = getRadius();
        float loss = LossFunctions.epsilonInsensitiveLoss(l2norm, epsilon);
        return (loss > 0.f) ? loss : 0.f;
    }

    @Override
    protected boolean learnRadius() {
        return B > 0.f;
    }

    protected float getRadius() {
        if(learn_radius) {// sqrt(B^2-W_{t,n+1}^2)
            assert (radiusKey != null);
            float bb = B * B;
            WeightValue w = weights.get(radiusKey); // decrease over time
            assert (w != null);
            float wt = w.get();
            if(wt == B) {
                return 0.f; // e_1 = 0;
            }
            float ww = wt * wt;
            float diff = bb - ww;
            if(diff < 0.f) {
                throw new IllegalStateException("Detected unexpected |w| > B found that w = " + wt
                        + ", B = " + B);
            }
            return (float) Math.sqrt(diff); // increase over time
        } else {
            return epsilon;
        }
    }

    @Override
    protected void update(List<?> features, float loss, PredictionResult margin) {
        float eta = eta(loss, margin);
        if(!Float.isInfinite(eta)) {
            update(features, eta);
        }
    }

    /** returns learning rate */
    protected float eta(float loss, PredictionResult margin) {
        return loss / margin.getL2Norm();
    }

    @Override
    public void close() throws HiveException {
        float radius = getRadius();
        weights.put(radiusKey, new WeightValue(radius));
        super.close();
    }

    public static class PA1 extends UniclassPassiveAggressiveUDTF {

        /** Aggressiveness parameter */
        protected float c;

        @Override
        protected Options getOptions() {
            Options opts = super.getOptions();
            opts.addOption("c", "aggressiveness", true, "Aggressiveness parameter C [default 1.0]");
            return opts;
        }

        @Override
        protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
            final CommandLine cl = super.processOptions(argOIs);

            float c = 1.f;
            if(cl != null) {
                String c_str = cl.getOptionValue("c");
                if(c_str != null) {
                    c = Float.parseFloat(c_str);
                    if(!(c > 0.f)) {
                        throw new UDFArgumentException("Aggressiveness parameter C must be C > 0: "
                                + c);
                    }
                }
            }

            this.c = c;
            return cl;
        }

        @Override
        protected float eta(float loss, PredictionResult margin) {
            float tau = loss / margin.getL2Norm();
            return Math.min(c, tau);
        }

    }

    public static class PA2 extends PA1 {

        @Override
        protected float eta(float loss, PredictionResult margin) {
            float tau = loss / margin.getL2Norm();
            return tau / (1.f + (0.5f / c));
        }
    }
}
