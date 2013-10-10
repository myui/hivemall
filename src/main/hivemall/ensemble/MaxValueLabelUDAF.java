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
package hivemall.ensemble;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

public class MaxValueLabelUDAF extends UDAF {

    public static class Evaluator implements UDAFEvaluator {

        public static class PartialResult {
            double maxValue;
            String label;

            void init() {
                this.maxValue = Double.NEGATIVE_INFINITY;
                this.label = null;
            }

            @Override
            public String toString() {
                return "PartialResult [maxValue=" + maxValue + ", label=" + label + "]";
            }
        }

        private PartialResult partial;

        @Override
        public void init() {
            this.partial = null;
        }

        public boolean iterate(double v, String label) {
            if(partial == null) {
                this.partial = new PartialResult();
                partial.init();
            }
            if(v >= partial.maxValue) {
                partial.maxValue = v;
                partial.label = label;
            }
            return true;
        }

        public PartialResult terminatePartial() {
            return partial;
        }

        public boolean merge(PartialResult other) {
            if(other == null) {
                return true;
            }
            if(partial == null) {
                this.partial = new PartialResult();
                partial.init();
            }
            if(other.maxValue < partial.maxValue) {
                return true;
            }
            partial.maxValue = other.maxValue;
            partial.label = other.label;
            return true;
        }

        public String terminate() {
            if(partial == null) {
                return null; // null to indicate that no values have been aggregated yet
            }
            return partial.label;
        }
    }
}
