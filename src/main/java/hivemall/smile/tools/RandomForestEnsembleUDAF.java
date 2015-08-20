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
package hivemall.smile.tools;

import hivemall.utils.lang.Counter;

import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.IntWritable;

@Description(name = "rf_ensemble", value = "_FUNC_(int y) - Returns emsebled prediction results of Random Forest classifiers")
public final class RandomForestEnsembleUDAF extends UDAF {

    public static class RandomForestPredictUDAFEvaluator implements UDAFEvaluator {

        private Counter<Integer> partial;

        public void init() {
            this.partial = null;
        }

        public boolean iterate(Integer x) {
            if(x == null) {
                return true;
            }
            if(partial == null) {
                this.partial = new Counter<Integer>();
            }
            partial.increment(x.intValue());
            return true;
        }

        /**
         * @see https://cwiki.apache.org/confluence/display/Hive/GenericUDAFCaseStudy#GenericUDAFCaseStudy-terminatePartial
         */
        public Map<Integer, Integer> terminatePartial() {
            if(partial == null) {
                return null;
            }

            if(partial.size() == 0) {
                return null;
            } else {
                return partial.getMap(); // CAN NOT return Counter here
            }
        }

        public boolean merge(Map<Integer, Integer> o) {
            if(o == null) {
                return true;
            }

            if(partial == null) {
                this.partial = new Counter<Integer>(o);
            } else {
                partial.addAll(o);
            }
            return true;
        }

        public IntWritable terminate() {
            if(partial == null) {
                return null;
            }
            if(partial.size() == 0) {
                return null;
            }

            Integer maxKey = partial.whichMax();
            if(maxKey == null) {
                return null;
            }
            return new IntWritable(maxKey.intValue());
        }
    }

}
