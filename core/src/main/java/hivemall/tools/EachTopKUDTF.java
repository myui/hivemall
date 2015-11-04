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
package hivemall.tools;

import hivemall.utils.collections.BoundedPriorityQueue;
import hivemall.utils.hadoop.HiveUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.IntWritable;

@Description(name = "each_top_k", value = "_FUNC_(const int K, Object group, double cmpKey, *) - Returns top-K values (or tail-K values when k is less than 0)")
public final class EachTopKUDTF extends GenericUDTF {

    private transient ObjectInspector[] argOIs;
    private transient ObjectInspector prevGroupOI;
    private transient PrimitiveObjectInspector cmpKeyOI;
    private int sizeK;

    private BoundedPriorityQueue<TupleWithKey> queue;
    private TupleWithKey _tuple;
    private Object _previousGroup;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        final int numArgs = argOIs.length;
        if(numArgs < 4) {
            throw new UDFArgumentException("each_top_k(const int K, Object group, double cmpKey, *) takes at least 4 arguments: "
                    + numArgs);
        }

        this.argOIs = argOIs;
        int k = HiveUtils.getAsConstInt(argOIs[0]);
        if(k == 0) {
            throw new UDFArgumentException("k should not be 0");
        }
        this.prevGroupOI = ObjectInspectorUtils.getStandardObjectInspector(argOIs[1], ObjectInspectorCopyOption.DEFAULT);
        this.cmpKeyOI = HiveUtils.asDoubleCompatibleOI(argOIs[2]);

        this.sizeK = Math.abs(k);
        final Comparator<TupleWithKey> comparator;
        if(k < 0) {
            comparator = Collections.reverseOrder();
        } else {
            comparator = new Comparator<TupleWithKey>() {
                @Override
                public int compare(TupleWithKey o1, TupleWithKey o2) {
                    return o1.compareTo(o2);
                }
            };
        }
        //this.queue = new BoundedPriorityQueue<Row>(sizeK, Comparator.nullsFirst(comparator));
        this.queue = new BoundedPriorityQueue<TupleWithKey>(sizeK, comparator);
        this._tuple = null;
        this._previousGroup = null;

        final ArrayList<String> fieldNames = new ArrayList<String>(numArgs);
        final ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(numArgs);
        fieldNames.add("rank");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        fieldNames.add("key");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        for(int i = 3; i < numArgs; i++) {
            fieldNames.add("c" + (i - 2));
            ObjectInspector rawOI = argOIs[i];
            ObjectInspector retOI = ObjectInspectorUtils.getStandardObjectInspector(rawOI, ObjectInspectorCopyOption.DEFAULT);
            fieldOIs.add(retOI);
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        final Object arg1 = args[1];
        if(isSameGroup(arg1) == false) {
            Object group = ObjectInspectorUtils.copyToStandardObject(arg1, argOIs[1], ObjectInspectorCopyOption.DEFAULT); // arg1 and group may be null
            drainQueue();
            this._previousGroup = group;
        }

        final double key = PrimitiveObjectInspectorUtils.getDouble(args[2], cmpKeyOI);
        final Object[] row;
        TupleWithKey tuple = this._tuple;
        if(_tuple == null) {
            row = new Object[args.length - 1];
            tuple = new TupleWithKey(key, row);
            this._tuple = tuple;
        } else {
            row = tuple.getRow();
            tuple.setKey(key);
        }
        for(int i = 3; i < args.length; i++) {
            Object arg = args[i];
            ObjectInspector argOI = argOIs[i];
            row[i - 1] = ObjectInspectorUtils.copyToStandardObject(arg, argOI, ObjectInspectorCopyOption.DEFAULT);
        }

        if(queue.offer(tuple)) {
            this._tuple = null;
        }
    }

    private boolean isSameGroup(Object arg1) {
        if(arg1 == null && _previousGroup == null) {
            return true;
        } else if(arg1 == null || _previousGroup == null) {
            return false;
        }
        return ObjectInspectorUtils.compare(arg1, argOIs[1], _previousGroup, prevGroupOI) == 0;
    }

    private void drainQueue() throws HiveException {
        final int queueSize = queue.size();
        if(queueSize > 0) {
            final TupleWithKey[] tuples = new TupleWithKey[queueSize];
            for(int i = 0; i < queueSize; i++) {
                TupleWithKey tuple = queue.poll();
                if(tuple == null) {
                    throw new IllegalStateException("Found null element in the queue");
                }
                tuples[i] = tuple;
            }
            final IntWritable rankProbe = new IntWritable(-1);
            final DoubleWritable keyProbe = new DoubleWritable(Double.NaN);
            int rank = 0;
            double lastKey = Double.NaN;
            for(int i = queueSize - 1; i >= 0; i--) {
                TupleWithKey tuple = tuples[i];
                tuples[i] = null; // help GC
                double key = tuple.getKey();
                if(key != lastKey) {
                    ++rank;
                    rankProbe.set(rank);
                    keyProbe.set(key);
                    lastKey = key;
                }
                Object[] row = tuple.getRow();
                row[0] = rankProbe;
                row[1] = keyProbe;
                forward(row);
            }
            queue.clear();
        }
    }

    @Override
    public void close() throws HiveException {
        drainQueue();

        this.queue = null;
        this._tuple = null;
    }

    private static final class TupleWithKey implements Comparable<TupleWithKey> {
        double key;
        Object[] row;

        TupleWithKey(double key, Object[] row) {
            this.key = key;
            this.row = row;
        }

        double getKey() {
            return key;
        }

        Object[] getRow() {
            return row;
        }

        void setKey(final double key) {
            this.key = key;
        }

        @Override
        public int compareTo(TupleWithKey o) {
            return Double.compare(key, o.key);
        }

    }

}
