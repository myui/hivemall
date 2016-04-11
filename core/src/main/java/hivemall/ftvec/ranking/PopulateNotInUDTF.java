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
package hivemall.ftvec.ranking;

import hivemall.UDTFWithOptions;
import hivemall.utils.hadoop.HiveUtils;

import java.util.ArrayList;
import java.util.BitSet;

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

@Description(
        name = "populate_not_in",
        value = "_FUNC_(list items, const int max_item_id [, const string options])"
                + "- Returns a relation consists of <int item> that item does not exist in the given items")
public final class PopulateNotInUDTF extends UDTFWithOptions {

    private ListObjectInspector listOI;
    private PrimitiveObjectInspector listElemOI;

    private int maxItemId;
    private boolean bitsetInput;

    private Object[] forwardObjs;
    private IntWritable populatedItemId;
    private BitSet _bitset;

    public PopulateNotInUDTF() {}

    @Override
    protected Options getOptions() {
        Options opts = new Options();
        opts.addOption("bitset", "bitset_input", false,
            "Use Bitset for the input of pos_items [default:false]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        CommandLine cl = null;

        boolean bitsetInput = false;
        if (argOIs.length == 3) {
            String args = HiveUtils.getConstString(argOIs[2]);
            cl = parseOptions(args);

            bitsetInput = cl.hasOption("bitset_input");
        }

        this.bitsetInput = bitsetInput;
        return cl;
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if (argOIs.length != 2 && argOIs.length != 3) {
            throw new UDFArgumentException(
                "bpr_sampling(array<long> items, const int max_item_id [, const string options])"
                        + " takes at least two arguments");
        }
        this.listOI = HiveUtils.asListOI(argOIs[0]);
        this.listElemOI = HiveUtils.asPrimitiveObjectInspector(listOI.getListElementObjectInspector());
        processOptions(argOIs);

        this.maxItemId = HiveUtils.getAsConstInt(argOIs[1]);
        if (maxItemId <= 0) {
            throw new UDFArgumentException("maxItemId MUST be greater than 0: " + maxItemId);
        }

        this.populatedItemId = new IntWritable();
        this.forwardObjs = new Object[] {populatedItemId};

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("item_id");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        Object arg0 = args[0];
        if (arg0 == null || listOI.getListLength(arg0) == 0) {
            populateAll();
        }

        final BitSet bits;
        if (bitsetInput) {
            long[] longs = HiveUtils.asLongArray(arg0, listOI, listElemOI);
            bits = BitSet.valueOf(longs);
        } else {
            if (_bitset == null) {
                bits = new BitSet();
                this._bitset = bits;
            } else {
                bits = _bitset;
                bits.clear();
            }
            HiveUtils.setBits(arg0, listOI, listElemOI, bits);
        }

        populateItems(bits);
    }

    private void populateItems(@Nonnull BitSet bits) throws HiveException {
        for (int i = bits.nextClearBit(0); i <= maxItemId; i = bits.nextClearBit(i + 1)) {
            populatedItemId.set(i);
            forward(forwardObjs);
        }
    }

    private void populateAll() throws HiveException {
        for (int i = 0; i <= maxItemId; i++) {
            populatedItemId.set(i);
            forward(forwardObjs);
        }
    }

    @Override
    public void close() throws HiveException {
        this.listOI = null;
        this.listElemOI = null;
        this.forwardObjs = null;
        this.populatedItemId = null;
        this._bitset = null;
    }

}
