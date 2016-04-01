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
package hivemall.ftvec.sampling;

import hivemall.utils.hadoop.HiveUtils;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Random;

import javax.annotation.Nonnull;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

@Description(
        name = "bpr_sampling",
        value = "_FUNC_(array<int> pos_items, const int max_item_id [, const double sampling_rates=1.0, const boolean withReplacement=true])"
                + "- Returns a relation consists of <int pos_item_id, int neg_item_id>")
public final class BprSamplingUDTF extends GenericUDTF {

    private ListObjectInspector listOI;
    private PrimitiveObjectInspector listElemOI;
    private int maxItemId;
    private float samplingRate;
    private boolean withoutReplacement;

    private Object[] forwardObjs;
    private IntWritable posItemId;
    private IntWritable negItemId;

    private BitSet bitset;
    private Random rand;

    public BprSamplingUDTF() {}

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if (argOIs.length < 2 || argOIs.length > 4) {
            throw new UDFArgumentException("bpr_sampling(array<long>, const long max_item_id "
                    + "[, const double sampling_rate, const boolean withoutReplacement=false])"
                    + " takes at least two arguments");
        }
        this.listOI = HiveUtils.asListOI(argOIs[0]);
        this.listElemOI = HiveUtils.asPrimitiveObjectInspector(listOI.getListElementObjectInspector());

        this.maxItemId = HiveUtils.getAsConstInt(argOIs[1]);
        if (maxItemId <= 0) {
            throw new UDFArgumentException("maxItemId MUST be greater than 0: " + maxItemId);
        }

        if (argOIs.length == 4) {
            this.withoutReplacement = HiveUtils.getConstBoolean(argOIs[3]);
        } else {
            this.withoutReplacement = false;
        }

        float rate = 1.f;
        if (argOIs.length >= 3) {
            rate = HiveUtils.getAsConstFloat(argOIs[2]);
            if (rate <= 0.f) {
                throw new UDFArgumentException("sampling_rate MUST be greater than 0: " + rate);
            }
            if (withoutReplacement && rate > 1.f) {
                throw new UDFArgumentException(
                    "sampling_rate MUST be in less than or equals to 1 where withoutReplacement is true: "
                            + rate);
            }
        }
        this.samplingRate = rate;

        this.posItemId = new IntWritable();
        this.negItemId = new IntWritable();
        this.forwardObjs = new Object[] {posItemId, negItemId};

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("pos_item");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        fieldNames.add("neg_item");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        if (bitset == null) {
            this.bitset = new BitSet();
            this.rand = new Random(43);
        } else {
            bitset.clear();
        }

        final int numPosItems = HiveUtils.setBits(args[0], listOI, listElemOI, bitset);
        if (numPosItems == 0) {
            return;
        }
        final int numNegItems = maxItemId + 1 - numPosItems;
        if (numNegItems == 0) {
            return;
        } else if (numNegItems < 0) {
            throw new UDFArgumentException("maxItemId + 1 - numPosItems = " + maxItemId + " + 1 - "
                    + numPosItems + " = " + numNegItems);
        }

        if (withoutReplacement) {
            sampleWithoutReplacement(numPosItems, numNegItems, bitset);
        } else {
            sampleWithReplacement(numPosItems, numNegItems, bitset);
        }
    }

    private void sampleWithoutReplacement(int numPosItems, int numNegItems,
            @Nonnull final BitSet bitset) throws HiveException {
        final BitSet bitsetForPosSampling = bitset;
        final BitSet bitsetForNegSampling = new BitSet();
        bitsetForPosSampling.or(bitset);

        final int numSamples = Math.max(1, Math.round(numPosItems * samplingRate));
        for (int s = 0; s < numSamples; s++) {
            int nth = rand.nextInt(numPosItems);

            int i = bitsetForPosSampling.nextSetBit(0);
            for (int c = 0; i >= 0; i = bitsetForPosSampling.nextSetBit(i + 1), c++) {
                if (c == nth) {
                    break;
                }
            }
            if (i == -1) {
                throw new UDFArgumentException("Illegal i value: " + i);
            }
            bitsetForPosSampling.set(i, false);
            --numPosItems;

            nth = rand.nextInt(numNegItems);
            int j = bitsetForNegSampling.nextClearBit(0);
            for (int c = 0; j <= maxItemId; j = bitsetForNegSampling.nextClearBit(j + 1), c++) {
                if (c == nth) {
                    break;
                }
            }
            if (j < 0 || j > maxItemId) {
                throw new UDFArgumentException("j MUST be in [0," + maxItemId + "] but j was " + j);
            }
            bitsetForNegSampling.set(j, true);
            --numNegItems;

            posItemId.set(i);
            negItemId.set(j);
            forward(forwardObjs);

            if (numPosItems <= 0) {
                // cannot draw a positive example anymore
                return;
            } else if (numNegItems <= 0) {
                // cannot draw a negative example anymore
                return;
            }
        }
    }

    private void sampleWithReplacement(final int numPosItems, final int numNegItems,
            @Nonnull final BitSet bitset) throws HiveException {
        final int numSamples = Math.max(1, Math.round(numPosItems * samplingRate));
        for (int s = 0; s < numSamples; s++) {
            int nth = rand.nextInt(numPosItems);

            int i = bitset.nextSetBit(0);
            for (int c = 0; i >= 0; i = bitset.nextSetBit(i + 1), c++) {
                if (c == nth) {
                    break;
                }
            }
            if (i == -1) {
                throw new UDFArgumentException("Illegal i value: " + i);
            }

            nth = rand.nextInt(numNegItems);
            int j = bitset.nextClearBit(0);
            for (int c = 0; j <= maxItemId; j = bitset.nextClearBit(j + 1), c++) {
                if (c == nth) {
                    break;
                }
            }
            if (j < 0 || j > maxItemId) {
                throw new UDFArgumentException("j MUST be in [0," + maxItemId + "] but j was " + j);
            }

            posItemId.set(i);
            negItemId.set(j);
            forward(forwardObjs);
        }
    }

    @Override
    public void close() throws HiveException {
        this.listOI = null;
        this.listElemOI = null;
        this.forwardObjs = null;
        this.posItemId = null;
        this.negItemId = null;
        this.bitset = null;
        this.rand = null;
    }

}
