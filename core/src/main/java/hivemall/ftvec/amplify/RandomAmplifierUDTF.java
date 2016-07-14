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
package hivemall.ftvec.amplify;

import hivemall.HivemallConstants;
import hivemall.UDTFWithOptions;
import hivemall.common.RandomizedAmplifier;
import hivemall.common.RandomizedAmplifier.DropoutListener;
import hivemall.utils.hadoop.HiveUtils;

import java.util.ArrayList;

import hivemall.utils.lang.Primitives;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

@Description(name = "rand_amplify", value = "_FUNC_(const int xtimes, const int num_buffers, [options], *)"
        + " - amplify the input records x-times in map-side")
public final class RandomAmplifierUDTF extends UDTFWithOptions implements DropoutListener<Object[]> {

    private boolean useSeed;
    private long seed;
    private boolean hasSeedOption = false;

    private transient ObjectInspector[] argOIs;
    private transient RandomizedAmplifier<Object[]> amplifier;

    @Override
    protected Options getOptions() {
        Options opts = new Options();
        opts.addOption("seed", true, "Seed value [default value is 42]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        CommandLine cl = null;
        if (argOIs.length >= 3 && HiveUtils.isConstString(argOIs[2])) {
            String rawArgs = HiveUtils.getConstString(argOIs[2]);
            cl = parseOptions(rawArgs);
            if (cl.hasOption("seed")) {
                useSeed = true;
                hasSeedOption = true;
                seed = Primitives.parseLong(cl.getOptionValue("seed"), HivemallConstants.DEFAULT_RAND_AMPLIFY_SEED);
            }
        }

        return cl;
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        final int numArgs = argOIs.length;
        if (numArgs < 3) {
            throw new UDFArgumentException(
                "_FUNC_(int xtimes, int num_buffers, *) takes at least three arguments");
        }
        // xtimes
        int xtimes = HiveUtils.getAsConstInt(argOIs[0]);
        if (!(xtimes >= 1)) {
            throw new UDFArgumentException("Illegal xtimes value: " + xtimes);
        }
        // num_buffers
        int numBuffers = HiveUtils.getAsConstInt(argOIs[1]);
        if (numBuffers < 2) {
            throw new UDFArgumentException("num_buffers must be greater than 2: " + numBuffers);
        }
        this.argOIs = argOIs;

        processOptions(argOIs);

        this.amplifier = useSeed ? new RandomizedAmplifier<Object[]>(numBuffers, xtimes, seed)
                : new RandomizedAmplifier<Object[]>(numBuffers, xtimes);
        amplifier.setDropoutListener(this);

        if (useSeed) {
            LogFactory.getLog(RandomAmplifierUDTF.class).info("rand_amplify() using seed: " + seed);
        }

        final ArrayList<String> fieldNames = new ArrayList<String>();
        final ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        int argStartIndex = hasSeedOption ? 3 : 2;
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
        int argStartIndex = hasSeedOption ? 3 : 2;
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
