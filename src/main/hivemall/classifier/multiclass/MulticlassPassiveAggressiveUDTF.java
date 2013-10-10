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
package hivemall.classifier.multiclass;

import hivemall.common.Margin;

import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class MulticlassPassiveAggressiveUDTF extends MulticlassOnlineClassifierUDTF {

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        final int numArgs = argOIs.length;
        if(numArgs != 2 && numArgs != 3) {
            throw new UDFArgumentException("MulticlassPassiveAggressiveUDTF takes 2 or 3 arguments: List<Text|Int|BitInt> features, {Int|Text} label [, constant text options]");
        }

        return super.initialize(argOIs);
    }

    @Override
    protected void train(List<?> features, Object actual_label) {
        assert (!features.isEmpty());

        Margin margin = getMargin(features, actual_label);
        float loss = loss(margin);

        if(loss > 0.f) { // & missed_label != null
            float sqnorm = squaredNorm(features);
            if(sqnorm == 0.f) {// avoid divide by zero
                return;
            }
            float coeff = eta(loss, sqnorm);
            Object missed_label = margin.getMaxIncorrectLabel();
            update(features, coeff, actual_label, missed_label);
        }
    }

    protected float loss(Margin margin) {
        return 1.f - margin.get();
    }

    protected float eta(float loss, float sqnorm) {
        return loss / sqnorm;
    }

    public static class PA1 extends MulticlassPassiveAggressiveUDTF {

        /** Aggressiveness parameter */
        protected float c;

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
        protected float eta(float loss, float sqnorm) {
            float eta = loss / sqnorm;
            return Math.min(c, eta);
        }

    }

    public static class PA2 extends PA1 {

        @Override
        protected float eta(float loss, float sqnorm) {
            float eta = loss / (sqnorm + (0.5f / c));
            return eta;
        }

    }
}
