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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.FloatWritable;

@Description(name = "argmin_kld", value = "_FUNC_(mean, covar) - Returns mean or covar that minimize a KL-distance among distributions", extended = "The returned value is (1.0 / (sum(1.0 / covar))) * (sum(mean / covar)")
public final class ArgminKLDistanceUDAF extends UDAF {

    public static class ArgminMeanUDAFEvaluator implements UDAFEvaluator {

        private PartialResult partial;

        public static class PartialResult {
            float sum_mean_div_covar;
            float sum_inv_covar;

            PartialResult() {
                this.sum_mean_div_covar = 0f;
                this.sum_inv_covar = 0f;
            }
        }

        public void init() {
            this.partial = null;
        }

        public boolean iterate(FloatWritable mean, FloatWritable covar) {
            if(mean == null || covar == null) {
                return true;
            }
            if(partial == null) {
                this.partial = new PartialResult();
            }
            float covar_f = covar.get();
            partial.sum_mean_div_covar += (mean.get() / covar_f);
            partial.sum_inv_covar += (1.f / covar_f);
            return true;
        }

        public PartialResult terminatePartial() {
            return partial;
        }

        public boolean merge(PartialResult o) {
            if(o == null) {
                return true;
            }
            if(partial == null) {
                this.partial = new PartialResult();
            }
            partial.sum_mean_div_covar += o.sum_mean_div_covar;
            partial.sum_inv_covar += o.sum_inv_covar;
            return true;
        }

        public FloatWritable terminate() {
            if(partial == null) {
                return null;
            }
            float mean = (1f / partial.sum_inv_covar) * partial.sum_mean_div_covar;
            return new FloatWritable(mean);
        }
    }

}
