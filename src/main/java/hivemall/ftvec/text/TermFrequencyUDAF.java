/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013-2014
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
package hivemall.ftvec.text;

import hivemall.utils.lang.mutable.MutableInt;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

public final class TermFrequencyUDAF extends UDAF {

    public static class Evaluator implements UDAFEvaluator {

        public static class PartialResult {

            private final Map<Text, MutableInt> map;
            private long globalCount;

            public PartialResult() {
                this.map = new HashMap<Text, MutableInt>();
                this.globalCount = 0L;
            }

        }

        private PartialResult partial;

        @Override
        public void init() {
            this.partial = null;
        }

        public boolean iterate(Text term) {
            if(term == null) {
                return true;
            }

            if(partial == null) {
                this.partial = new PartialResult();
                partial.map.put(term, new MutableInt(1));
            } else {
                final Map<Text, MutableInt> map = partial.map;
                MutableInt count = map.get(term);
                if(count == null) {
                    map.put(term, new MutableInt(1));
                } else {
                    int newcount = count.getValue() + 1;
                    count.setValue(newcount);
                }
            }
            partial.globalCount++;
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
            }
            final Map<Text, MutableInt> this_map = partial.map;
            final Map<Text, MutableInt> other_map = other.map;
            for(Map.Entry<Text, MutableInt> e : other_map.entrySet()) {
                Text term = e.getKey();
                MutableInt other_count = e.getValue();
                MutableInt this_count = this_map.get(term);
                if(this_count == null) {
                    this_map.put(term, other_count);
                } else {
                    int newcount = this_count.getValue() + other_count.getValue();
                    this_count.setValue(newcount);
                }
            }
            partial.globalCount += other.globalCount;
            return true;
        }

        public Map<Text, FloatWritable> terminate() {
            if(partial == null) {
                return null;
            }
            final long globalCount = partial.globalCount;
            final Map<Text, FloatWritable> tfmap = new HashMap<Text, FloatWritable>();
            for(Map.Entry<Text, MutableInt> e : partial.map.entrySet()) {
                Text term = e.getKey();
                float other_count = e.getValue().getValue();
                float freq = other_count / globalCount;
                tfmap.put(term, new FloatWritable(freq));
            }
            this.partial = null;
            return tfmap;
        }
    }

}
