/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package hivemall.ftvec.conv;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.FloatWritable;

@SuppressWarnings("deprecation")
@Description(
        name = "conv2dense",
        value = "_FUNC_(int feature, float weight, int nDims) - Return a dense model in array<float>")
public class ConvertToDenseModelUDAF extends UDAF {

    public static class Evaluator implements UDAFEvaluator {

        private List<FloatWritable> partial;

        @Override
        public void init() {
            this.partial = null;
        }

        public boolean iterate(int feature, float weight, int nDims) {
            if (partial == null) {
                FloatWritable[] array = new FloatWritable[nDims];
                this.partial = Arrays.asList(array);
            }
            partial.set(feature, new FloatWritable(weight));
            return true;
        }

        public List<FloatWritable> terminatePartial() {
            return partial;
        }

        public boolean merge(List<FloatWritable> other) {
            if (other == null) {
                return true;
            }
            if (partial == null) {
                this.partial = new ArrayList<FloatWritable>(other);
                return true;
            }
            final int nDims = other.size();
            for (int i = 0; i < nDims; i++) {
                FloatWritable x = other.set(i, null);
                if (x != null) {
                    partial.set(i, x);
                }
            }
            return true;
        }

        public List<FloatWritable> terminate() {
            if (partial == null) {
                return null; // null to indicate that no values have been aggregated yet
            }
            return partial;
        }

    }
}
