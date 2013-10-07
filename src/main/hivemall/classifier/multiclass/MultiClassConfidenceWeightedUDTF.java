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
import hivemall.utils.StatsUtils;

import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * A multi-class confidence-weighted linear classification.
 * <pre>
 * [1] Mark Dredze, Koby Crammer and Fernando Pereira. "Confidence-weighted linear classification",
 *     In Proc. ICML, pp.264-271, 2008.
 * [2] Koby Crammer, Mark Dredze and Alex Kulesza. "Multi-class confidence weighted algorithms",
 *     In Proc. EMNLP, Vol. 2, pp.496-504, 2008.
 * </pre>
 * 
 * @link http://dl.acm.org/citation.cfm?id=1390190
 * @link http://dl.acm.org/citation.cfm?id=1699577
 */
public class MultiClassConfidenceWeightedUDTF extends MulticlassOnlineClassifierUDTF {

    /** confidence parameter phi */
    protected float phi;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        final int numArgs = argOIs.length;
        if(numArgs != 2 && numArgs != 3) {
            throw new UDFArgumentException("MultiClassConfidenceWeightedUDTF takes 2 or 3 arguments: List<String|Int|BitInt> features, {Int|String} label [, constant String options]");
        }

        return super.initialize(argOIs);
    }

    @Override
    protected Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("phi", "confidence", true, "Confidence parameter [default 0.5]");
        opts.addOption("eta", "hyper_c", true, "Confidence hyperparameter eta in range (0, 1) [default 0.7]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        final CommandLine cl = super.processOptions(argOIs);

        float phi = 0.5f;
        if(cl != null) {
            String phi_str = cl.getOptionValue("phi");
            if(phi_str == null) {
                String eta_str = cl.getOptionValue("eta");
                if(eta_str != null) {
                    double eta = Double.parseDouble(eta_str);
                    if(eta <= 0 || eta >= 1) {
                        throw new UDFArgumentException("Confidence hyperparameter eta must be in range (0,1): "
                                + eta_str);
                    }
                    phi = (float) StatsUtils.probit(eta, 10d);
                }
            } else {
                phi = Float.parseFloat(phi_str);
            }
        }

        this.phi = phi;
        return cl;
    }

    @Override
    protected void train(List<?> features, Object actual_label) {
        Margin margin = getMarginAndVariance(features, actual_label);
        Object missed_label = margin.getMaxIncorrectLabel();

        float gamma = getGamma(margin);

        if(gamma > 0.f) {// alpha = max(0, gamma)            
            update(features, gamma, actual_label, missed_label, gamma, phi);
        }
    }

    protected final float getGamma(Margin margin) {
        float score = margin.get();
        float var = margin.getVariance();

        float b = 1.f + 2.f * phi * score;
        float gamma = (-b + (float) Math.sqrt(b * b - 8.f * phi * (score - phi * var))) / 4.f * phi
                * var;
        return gamma;
    }

}
