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
package hivemall.ftvec.amplify;

import hivemall.UDTFWithOptions;
import hivemall.common.RandomizedAmplifier;
import hivemall.common.RandomizedAmplifier.DropoutListener;
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.lang.Primitives;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

@Description(name = "rand_amplify", value = "_FUNC_(const int xtimes [, const string options], *)"
        + " - amplify the input records x-times in map-side")
public final class RandomAmplifierUDTF extends UDTFWithOptions implements DropoutListener<Object[]> {

    private boolean hasOption = false;
    private long seed = -1L;
    private int numBuffers = 1000;

    private transient ObjectInspector[] argOIs;
    private transient RandomizedAmplifier<Object[]> amplifier;

    @Override
    protected Options getOptions() {
        Options opts = new Options();
        opts.addOption("seed", true, "Random seed value [default: -1L (random)]");
        opts.addOption("buf", "num_buffers", true,
            "The number of rows to keep in a buffer [default: 1000]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        CommandLine cl = null;
        if (argOIs.length >= 3 && HiveUtils.isConstString(argOIs[1])) {
            String rawArgs = HiveUtils.getConstString(argOIs[1]);
            cl = parseOptions(rawArgs);
            this.hasOption = true;
            this.seed = Primitives.parseLong(cl.getOptionValue("seed"), this.seed);
            this.numBuffers = Primitives.parseInt(cl.getOptionValue("num_buffers"), this.numBuffers);
        }
        return cl;
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        final int numArgs = argOIs.length;
        if (numArgs < 2) {
            throw new UDFArgumentException(
                "_FUNC_(const int xtimes, [, const string options], *) takes at least two arguments");
        }
        // xtimes
        int xtimes = HiveUtils.getAsConstInt(argOIs[0]);
        if (xtimes < 1) {
            throw new UDFArgumentException("Illegal xtimes value: " + xtimes);
        }
        this.argOIs = argOIs;

        processOptions(argOIs);

        this.amplifier = (seed == -1L) ? new RandomizedAmplifier<Object[]>(numBuffers, xtimes)
                : new RandomizedAmplifier<Object[]>(numBuffers, xtimes, seed);
        amplifier.setDropoutListener(this);

        final List<String> fieldNames = new ArrayList<String>();
        final List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        final int argStartIndex = hasOption ? 2 : 1;
        for (int i = argStartIndex; i < numArgs; i++) {
            fieldNames.add("c" + (i - 1));
            ObjectInspector rawOI = argOIs[i];
            ObjectInspector retOI = ObjectInspectorUtils.getStandardObjectInspector(rawOI,
                ObjectInspectorCopyOption.DEFAULT);
            fieldOIs.add(retOI);
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        final int argStartIndex = hasOption ? 2 : 1;
        final Object[] row = new Object[args.length - argStartIndex];
        for (int i = argStartIndex; i < args.length; i++) {
            Object arg = args[i];
            ObjectInspector argOI = argOIs[i];
            row[i - argStartIndex] = ObjectInspectorUtils.copyToStandardObject(arg, argOI,
                ObjectInspectorCopyOption.DEFAULT);
        }
        amplifier.add(row);
    }

    @Override
    public void close() throws HiveException {
        amplifier.sweepAll();
        this.amplifier = null;
    }

    @Override
    public void onDrop(Object[] row) throws HiveException {
        forward(row);
    }

}
