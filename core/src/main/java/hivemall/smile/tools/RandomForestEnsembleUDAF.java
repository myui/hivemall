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

import hivemall.utils.collections.IntArrayList;
import hivemall.utils.lang.Counter;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

@Description(name = "rf_ensemble", value = "_FUNC_(int y) - Returns emsebled prediction results of Random Forest classifiers")
public final class RandomForestEnsembleUDAF extends UDAF {

    public static class RandomForestPredictUDAFEvaluator implements UDAFEvaluator {

        private Counter<Integer> partial;

        public void init() {
            this.partial = null;
        }

        public boolean iterate(Integer k) {
            if(k == null) {
                return true;
            }
            if(partial == null) {
                this.partial = new Counter<Integer>();
            }
            partial.increment(k);
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

        public Result terminate() {
            if(partial == null) {
                return null;
            }
            if(partial.size() == 0) {
                return null;
            }

            return new Result(partial);
        }
    }

    public static final class Result {
        @SuppressWarnings("unused")
        private Integer label;
        @SuppressWarnings("unused")
        private Double probability;
        @SuppressWarnings("unused")
        private List<Double> probabilities;

        Result(Counter<Integer> partial) {
            final Map<Integer, Integer> counts = partial.getMap();
            int size = counts.size();
            assert (size > 0) : size;
            IntArrayList keyList = new IntArrayList(size);

            long totalCnt = 0L;
            Integer maxKey = null;
            int maxCnt = Integer.MIN_VALUE;
            for(Map.Entry<Integer, Integer> e : counts.entrySet()) {
                Integer key = e.getKey();
                keyList.add(key);
                int cnt = e.getValue().intValue();
                totalCnt += cnt;
                if(cnt >= maxCnt) {
                    maxCnt = cnt;
                    maxKey = key;
                }
            }

            int[] keyArray = keyList.toArray();
            Arrays.sort(keyArray);
            int last = keyArray[keyArray.length - 1];

            double totalCnt_d = (double) totalCnt;
            final Double[] probabilities = new Double[Math.max(2, last + 1)];
            for(int i = 0, len = probabilities.length; i < len; i++) {
                final Integer cnt = counts.get(Integer.valueOf(i));
                if(cnt == null) {
                    probabilities[i] = Double.valueOf(0d);
                } else {
                    probabilities[i] = Double.valueOf(cnt.intValue() / totalCnt_d);
                }
            }
            this.label = maxKey;
            this.probability = Double.valueOf(maxCnt / totalCnt_d);
            this.probabilities = Arrays.asList(probabilities);
        }
    }

}
