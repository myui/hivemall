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
package hivemall.ensemble;

import hivemall.utils.hadoop.WritableUtils;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.Text;

public final class MaxValueLabelUDAF extends UDAF {

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

        public Text terminate() {
            if(partial == null) {
                return null; // null to indicate that no values have been aggregated yet
            }
            return WritableUtils.val(partial.label);
        }
    }
}
