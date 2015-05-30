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
package hivemall.knn.lsh;

import static hivemall.HivemallConstants.BIGINT_TYPE_NAME;
import static hivemall.HivemallConstants.INT_TYPE_NAME;
import static hivemall.HivemallConstants.STRING_TYPE_NAME;
import hivemall.UDTFWithOptions;
import hivemall.io.FeatureValue;
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.hashing.HashFunction;
import hivemall.utils.hashing.HashFunctionFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * A Minhash implementation that outputs n different k-depth Signatures.
 */
@UDFType(deterministic = true, stateful = false)
public final class MinHashUDTF extends UDTFWithOptions {

    private ObjectInspector itemOI;
    private ListObjectInspector featureListOI;
    private boolean parseFeature;
    private Object[] forwardObjs;

    private int num_hashes = 5;
    private int num_keygroups = 2;
    private HashFunction[] hashFuncs;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if(argOIs.length < 2) {
            throw new UDFArgumentException(getClass().getSimpleName()
                    + " takes more than 2 arguments: ANY item, Array<Int|BigInt|Text> features [, constant String options]");
        }
        this.itemOI = argOIs[0];

        this.featureListOI = (ListObjectInspector) argOIs[1];
        ObjectInspector featureRawOI = featureListOI.getListElementObjectInspector();
        String keyTypeName = featureRawOI.getTypeName();
        if(!STRING_TYPE_NAME.equals(keyTypeName) && !INT_TYPE_NAME.equals(keyTypeName)
                && !BIGINT_TYPE_NAME.equals(keyTypeName)) {
            throw new UDFArgumentTypeException(0, "1st argument must be Map of key type [Int|BitInt|Text]: "
                    + keyTypeName);
        }
        this.parseFeature = STRING_TYPE_NAME.equals(keyTypeName);
        this.forwardObjs = new Object[2];

        processOptions(argOIs);

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("clusterid");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        fieldNames.add("item");
        fieldOIs.add(itemOI);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    protected Options getOptions() {
        Options opts = new Options();
        opts.addOption("n", "hashes", true, "Generate N sets of minhash values for each row (DEFAULT: 5)");
        opts.addOption("k", "keygroups", true, "Use K minhash value (DEFAULT: 2)");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        CommandLine cl = null;
        if(argOIs.length >= 3) {
            String rawArgs = HiveUtils.getConstString(argOIs[2]);
            cl = parseOptions(rawArgs);

            String numHashes = cl.getOptionValue("hashes");
            if(numHashes != null) {
                this.num_hashes = Integer.parseInt(numHashes);
            }

            String numKeygroups = cl.getOptionValue("keygroups");
            if(numKeygroups != null) {
                this.num_keygroups = Integer.parseInt(numKeygroups);
            }
        }

        this.hashFuncs = HashFunctionFactory.create(num_hashes);

        return cl;
    }

    @Override
    public void process(Object[] args) throws HiveException {
        final Object[] forwardObjs = this.forwardObjs;
        forwardObjs[1] = args[0];
        List<?> features = (List<?>) featureListOI.getList(args[1]);
        ObjectInspector featureInspector = featureListOI.getListElementObjectInspector();
        List<FeatureValue> ftvec = parseFeatures(features, featureInspector, parseFeature);

        computeAndForwardSignatures(ftvec, forwardObjs);
    }

    private void computeAndForwardSignatures(List<FeatureValue> features, Object[] forwardObjs)
            throws HiveException {
        final PriorityQueue<Integer> minhashes = new PriorityQueue<Integer>();
        // Compute N sets K minhash values
        for(int i = 0; i < num_hashes; i++) {
            float weightedMinHashValues = Float.MAX_VALUE;

            for(FeatureValue fv : features) {
                Object f = fv.getFeature();
                int hashIndex = Math.abs(hashFuncs[i].hash(f));
                float w = fv.getValue();
                float hashValue = calcWeightedHashValue(hashIndex, w);
                if(hashValue < weightedMinHashValues) {
                    weightedMinHashValues = hashValue;
                    minhashes.offer(hashIndex);
                }
            }

            forwardObjs[0] = getSignature(minhashes, num_keygroups);
            forward(forwardObjs);

            minhashes.clear();
        }
    }

    /**
     * For a larger w, hash value tends to be smaller and tends to be selected as minhash. 
     */
    private static float calcWeightedHashValue(final int hashIndex, final float w)
            throws HiveException {
        if(w < 0.f) {
            throw new HiveException("Non-negative value is not accepted for a feature weight");
        }
        if(w == 0.f) {
            return Float.MAX_VALUE;
        } else {
            return hashIndex / w;
        }
    }

    private static int getSignature(PriorityQueue<Integer> candidates, int keyGroups) {
        final int numCandidates = candidates.size();
        if(numCandidates == 0) {
            return 0;
        }

        final int size = Math.min(numCandidates, keyGroups);
        int result = 1;
        for(int i = 0; i < size; i++) {
            int nextmin = candidates.poll();
            result = (31 * result) + nextmin;
        }
        return result & 0x7FFFFFFF;
    }

    @Override
    public void close() throws HiveException {}

}
