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
package hivemall.classifier;

import hivemall.common.LossFunctions;
import hivemall.common.PredictionResult;

import java.util.List;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class PassiveAggressiveUDTF extends BinaryOnlineClassifierUDTF {

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        final int numArgs = argOIs.length;
        if(numArgs != 2 && numArgs != 3) {
            throw new UDFArgumentException("PassiveAggressiveUDTF takes 2 or 3 arguments: List<Text|Int|BitInt> features, int label [, constant string options]");
        }

        return super.initialize(argOIs);
    }

    @Override
    protected void train(final List<?> features, final int label) {
        final int y = label > 0 ? 1 : -1;

        PredictionResult margin = calcScoreAndNorm(features);
        float p = margin.getScore();
        float loss = LossFunctions.hingeLoss(p, y); // 1.0 - y * p

        if(loss > 0.f) { // y * p < 1
            float eta = eta(loss, margin);
            float coeff = eta * y;
            update(features, coeff);
        }
    }

    /** returns learning rate */
    protected float eta(float loss, PredictionResult margin) {
        return loss / margin.getSquaredNorm();
    }

    public static class PA1 extends PassiveAggressiveUDTF {

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
            float squared_norm = margin.getSquaredNorm();
            float eta = loss / squared_norm;
            return Math.min(c, eta);
        }

    }

    public static class PA2 extends PA1 {

        @Override
        protected float eta(float loss, PredictionResult margin) {
            float squared_norm = margin.getSquaredNorm();
            float eta = loss / (squared_norm + (0.5f / c));
            return eta;
        }
    }
}
