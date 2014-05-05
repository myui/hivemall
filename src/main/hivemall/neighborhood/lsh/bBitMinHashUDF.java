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
 */package hivemall.neighborhood.lsh;

import hivemall.common.FeatureValue;
import hivemall.utils.hashing.MurmurHash3;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;

public class bBitMinHashUDF extends UDF {

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

    public Long evaluate(List<Integer> features) throws HiveException {
        return evaluate(features, 128);
    }

    public Long evaluate(List<Integer> features, int numHashes) throws HiveException {
        int[] seeds = prepareSeeds(numHashes);
        List<FeatureValue> featureList = parseFeatures(features);
        return computeSignatures(featureList, numHashes, seeds);
    }

    public Long evaluate(List<String> features, boolean noWeight) throws HiveException {
        return evaluate(features, 128, noWeight);
    }

    public Long evaluate(List<String> features, int numHashes, boolean noWeight)
            throws HiveException {
        int[] seeds = prepareSeeds(numHashes);
        List<FeatureValue> featureList = parseFeatures(features, noWeight);
        return computeSignatures(featureList, numHashes, seeds);
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
                fv = FeatureValue.parse(f, false);
            }
            ftvec.add(fv);
        }
        return ftvec;
    }

    private static long computeSignatures(final List<FeatureValue> features, final int numHashes, final int[] seeds)
            throws HiveException {
        if(numHashes <= 0 || numHashes > 512) {
            throw new HiveException("The number of hash function must be in range (0,512]: "
                    + numHashes);
        }
        final int[] hashes = new int[numHashes];
        // Compute N sets K minhash values
        for(int i = 0; i < numHashes; i++) {
            float weightedMinHashValues = Float.MAX_VALUE;
            for(FeatureValue fv : features) {
                Object f = fv.getFeature();
                assert (f != null);
                String fs = f.toString();
                int hashIndex = Math.abs(MurmurHash3.murmurhash3_x86_32(fs, seeds[i]));
                float w = fv.getValue();
                float hashValue = calcWeightedHashValue(hashIndex, w);
                if(hashValue < weightedMinHashValues) {
                    weightedMinHashValues = hashValue;
                    hashes[i] = hashIndex;
                }
            }
        }
        long value = 0L;
        for(int i = 0; i < numHashes; i++) {
            if((hashes[i] & 1) == 1) {
                value += (1L << i);
            }
        }
        return value;
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
}
