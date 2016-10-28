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
package hivemall.ftvec.ranking;

import hivemall.UDTFWithOptions;
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.lang.BitUtils;
import hivemall.utils.lang.Primitives;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Random;

import javax.annotation.Nonnull;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

@Description(name = "item_pairs_sampling",
        value = "_FUNC_(array<int|long> pos_items, const int max_item_id [, const string options])"
                + "- Returns a relation consists of <int pos_item_id, int neg_item_id>")
public final class ItemPairsSamplingUDTF extends UDTFWithOptions {

    private ListObjectInspector listOI;
    private PrimitiveObjectInspector listElemOI;

    private int maxItemId;
    private boolean bitsetInput;
    private float samplingRate;
    private boolean withReplacement;

    private Object[] forwardObjs;
    private IntWritable posItemId;
    private IntWritable negItemId;

    private BitSet _bitset;
    private Random _rand;

    public ItemPairsSamplingUDTF() {}

    @Override
    protected Options getOptions() {
        Options opts = new Options();
        opts.addOption("bitset", "bitset_input", false,
            "Use Bitset for the input of pos_items [default:false]");
        opts.addOption("sampling", "sampling_rate", true,
            "Sampling rates of positive items [default: 1.0]");
        opts.addOption("with_replacement", false, "Do sampling with-replacement [default: false]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        CommandLine cl = null;

        boolean bitsetInput = false;
        float samplingRate = 1.f;
        boolean withReplacement = false;
        if (argOIs.length == 3) {
            String args = HiveUtils.getConstString(argOIs[2]);
            cl = parseOptions(args);

            bitsetInput = cl.hasOption("bitset_input");
            withReplacement = cl.hasOption("with_replacement");
            samplingRate = Primitives.parseFloat(cl.getOptionValue("sampling_rate"), samplingRate);
            if (withReplacement == false && samplingRate > 1.f) {
                throw new UDFArgumentException(
                    "sampling_rate MUST be in less than or equals to 1 where withReplacement is false: "
                            + samplingRate);
            }
        }

        this.bitsetInput = bitsetInput;
        this.samplingRate = samplingRate;
        this.withReplacement = withReplacement;
        return cl;
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if (argOIs.length != 2 && argOIs.length != 3) {
            throw new UDFArgumentException(
                "_FUNC_(array<long>, const long max_item_id [, const string options])"
                        + " takes at least two arguments");
        }
        this.listOI = HiveUtils.asListOI(argOIs[0]);
        this.listElemOI = HiveUtils.asPrimitiveObjectInspector(listOI.getListElementObjectInspector());
        processOptions(argOIs);

        this.maxItemId = HiveUtils.getAsConstInt(argOIs[1]);
        if (maxItemId <= 0) {
            throw new UDFArgumentException("maxItemId MUST be greater than 0: " + maxItemId);
        }

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
        final int numPosItems;
        final BitSet bits;
        if (bitsetInput) {
            if (_rand == null) {
                this._rand = new Random(43);
            }
            long[] longs = HiveUtils.asLongArray(args[0], listOI, listElemOI);
            bits = BitSet.valueOf(longs);
            numPosItems = bits.cardinality();
        } else {
            if (_bitset == null) {
                bits = new BitSet();
                this._bitset = bits;
                this._rand = new Random(43);
            } else {
                bits = _bitset;
                bits.clear();
            }
            numPosItems = HiveUtils.setBits(args[0], listOI, listElemOI, bits);
        }

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

        if (withReplacement) {
            sampleWithReplacement(numPosItems, numNegItems, bits);
        } else {
            sampleWithoutReplacement(numPosItems, numNegItems, bits);
        }
    }

    private void sampleWithReplacement(final int numPosItems, final int numNegItems,
            @Nonnull final BitSet bitset) throws HiveException {
        final int numSamples = Math.max(1, Math.round(numPosItems * samplingRate));
        for (int s = 0; s < numSamples; s++) {
            int nth = _rand.nextInt(numPosItems);

            int i = BitUtils.indexOfSetBit(bitset, nth);
            if (i == -1) {
                throw new UDFArgumentException("Cannot find a value for " + nth
                        + "-th element in bitset " + bitset.toString() + " where numPosItems = "
                        + numPosItems);
            }

            nth = _rand.nextInt(numNegItems);
            int j = BitUtils.indexOfClearBit(bitset, nth, maxItemId);
            if (j < 0 || j > maxItemId) {
                throw new UDFArgumentException("j MUST be in [0," + maxItemId + "] but j was " + j);
            }

            posItemId.set(i);
            negItemId.set(j);
            forward(forwardObjs);
        }
    }

    private void sampleWithoutReplacement(int numPosItems, int numNegItems,
            @Nonnull final BitSet bitset) throws HiveException {
        final BitSet bitsetForPosSampling = bitset;
        final BitSet bitsetForNegSampling = BitSet.valueOf(bitset.toLongArray());

        final int numSamples = Math.max(1, Math.round(numPosItems * samplingRate));
        for (int s = 0; s < numSamples; s++) {
            int nth = _rand.nextInt(numPosItems);
            int i = BitUtils.indexOfSetBit(bitsetForPosSampling, nth);
            if (i == -1) {
                throw new UDFArgumentException("Cannot find a value for " + nth
                        + "-th element in bitset " + bitset.toString() + " where numPosItems = "
                        + numPosItems);
            }
            bitsetForPosSampling.set(i, false);
            --numPosItems;

            nth = _rand.nextInt(numNegItems);
            int j = BitUtils.indexOfClearBit(bitsetForNegSampling, nth, maxItemId);
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

    @Override
    public void close() throws HiveException {
        this.listOI = null;
        this.listElemOI = null;
        this.forwardObjs = null;
        this.posItemId = null;
        this.negItemId = null;
        this._bitset = null;
        this._rand = null;
    }

}
