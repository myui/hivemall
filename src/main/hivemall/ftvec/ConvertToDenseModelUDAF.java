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
package hivemall.ftvec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

public class ConvertToDenseModelUDAF extends UDAF {

    public static class Evaluator implements UDAFEvaluator {

        private List<Float> partial;

        @Override
        public void init() {
            this.partial = null;
        }

        public boolean iterate(int feature, float weight, int nDims) {
            if(partial == null) {
                Float[] array = new Float[nDims];
                this.partial = Arrays.asList(array);
            }
            partial.set(feature, new Float(weight));
            return true;
        }

        public List<Float> terminatePartial() {
            return partial;
        }

        public boolean merge(List<Float> other) {
            if(other == null) {
                return true;
            }
            if(partial == null) {
                this.partial = new ArrayList<Float>(other);
                return true;
            }
            final int nDims = other.size();
            for(int i = 0; i < nDims; i++) {
                Float x = other.set(i, null);
                if(x != null) {
                    partial.set(i, x);
                }
            }
            return true;
        }

        public List<Float> terminate() {
            if(partial == null) {
                return null; // null to indicate that no values have been aggregated yet
            }
            return partial;
        }

    }
}
