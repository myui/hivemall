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
package hivemall.ensemble.bagging;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

public class WeightVotedAvgUDAF extends UDAF {

    public static class Evaluator implements UDAFEvaluator {

        public static class PartialResult {

            double positiveSum;
            int positiveCnt;
            double negativeSum;
            int negativeCnt;

            void init() {
                positiveSum = 0d;
                positiveCnt = 0;
                negativeSum = 0d;
                negativeCnt = 0;
            }

        }

        private PartialResult partial;

        @Override
        public void init() {
            this.partial = null;
        }

        public boolean iterate(float w) {
            if(partial == null) {
                this.partial = new PartialResult();
                partial.init();
            }
            if(w > 0) {
                partial.positiveSum += (double) w;
                partial.positiveCnt++;
            } else if(w < 0) {
                partial.negativeSum += (double) w;
                partial.negativeCnt++;
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
            partial.positiveSum += other.positiveSum;
            partial.positiveCnt += other.positiveCnt;
            partial.negativeSum += other.negativeSum;
            partial.negativeCnt += other.negativeCnt;
            return true;
        }

        public Double terminate() {
            if(partial == null) {
                return null; // null to indicate that no values have been aggregated yet
            }
            if(partial.positiveSum > (-partial.negativeSum)) {
                return partial.positiveSum / partial.positiveCnt;
            } else {
                if(partial.negativeCnt == 0) {
                    assert (partial.negativeSum == 0d) : partial.negativeSum;
                    return 0.d;
                }
                return partial.negativeSum / partial.negativeCnt;
            }
        }
    }
}
