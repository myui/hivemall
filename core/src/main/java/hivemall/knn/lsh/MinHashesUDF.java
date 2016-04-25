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

import static hivemall.utils.hadoop.WritableUtils.val;
import hivemall.model.FeatureValue;
import hivemall.utils.hashing.MurmurHash3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.IntWritable;

@UDFType(deterministic = true, stateful = false)
public final class MinHashesUDF extends UDF {

    private int[] _seeds = null;

    private int[] prepareSeeds(final int numHashes) {
        int[] seeds = this._seeds;
        if(seeds == null || seeds.length != numHashes) {
            seeds = new int[numHashes];
            final Random rand = new Random(31L);
            for(int i = 0; i < numHashes; i++) {
                seeds[i] = rand.nextInt();
            }
            this._seeds = seeds;
        }
        return seeds;
    }

    public List<IntWritable> evaluate(List<Integer> features) throws HiveException {
        return evaluate(features, 5, 2);
    }

    public List<IntWritable> evaluate(List<Integer> features, int numHashes, int keyGroups)
            throws HiveException {
        int[] seeds = prepareSeeds(numHashes);
        List<FeatureValue> featureList = parseFeatures(features);
        return computeSignatures(featureList, numHashes, keyGroups, seeds);
    }

    public List<IntWritable> evaluate(List<String> features, boolean noWeight) throws HiveException {
        return evaluate(features, 5, 2, noWeight);
    }

    public List<IntWritable> evaluate(List<String> features, int numHashes, int keyGroups, boolean noWeight)
            throws HiveException {
        int[] seeds = prepareSeeds(numHashes);
        List<FeatureValue> featureList = parseFeatures(features, noWeight);
        return computeSignatures(featureList, numHashes, keyGroups, seeds);
    }

    private static List<FeatureValue> parseFeatures(final List<Integer> features) {
        final List<FeatureValue> ftvec = new ArrayList<FeatureValue>(features.size());
        for(Integer f : features) {
            if(f != null) {
                FeatureValue fv = new FeatureValue(f, 1.f);
                ftvec.add(fv);
            }
        }
        return ftvec;
    }

    private static List<FeatureValue> parseFeatures(final List<String> features, final boolean noWeight) {
        final List<FeatureValue> ftvec = new ArrayList<FeatureValue>(features.size());
        for(String f : features) {
            if(f == null) {
                continue;
            }
            final FeatureValue fv;
            if(noWeight) {
                fv = new FeatureValue(f, 1.f);
            } else {
                fv = FeatureValue.parse(f);
            }
            ftvec.add(fv);
        }
        return ftvec;
    }

    private static List<IntWritable> computeSignatures(final List<FeatureValue> features, final int numHashes, final int keyGroups, final int[] seeds)
            throws HiveException {
        final IntWritable[] hashes = new IntWritable[numHashes];
        final PriorityQueue<Integer> minhashes = new PriorityQueue<Integer>();
        // Compute N sets K minhash values
        for(int i = 0; i < numHashes; i++) {
            float weightedMinHashValues = Float.MAX_VALUE;
            for(FeatureValue fv : features) {
                Object f = fv.getFeature();
                assert (f != null);
                String fs = f.toString();
                int hashIndex = Math.abs(MurmurHash3.murmurhash3_x86_32(fs, seeds[i]));
                float w = fv.getValueAsFloat();
                float hashValue = calcWeightedHashValue(hashIndex, w);
                if(hashValue < weightedMinHashValues) {
                    weightedMinHashValues = hashValue;
                    minhashes.offer(hashIndex);
                }
            }

            hashes[i] = val(getSignature(minhashes, keyGroups));
            minhashes.clear();
        }

        return Arrays.asList(hashes);
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
}
