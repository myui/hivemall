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
package hivemall.common;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Vitter's reservoir sampling implementation that randomly chooses k items from a list containing n
 * items.
 * 
 * @link http://en.wikipedia.org/wiki/Reservoir_sampling
 * @link http://portal.acm.org/citation.cfm?id=3165
 */
public final class ReservoirSampler<T> {

    private final T[] samples;
    private final int numSamples;
    private int position;

    private final Random rand;

    @SuppressWarnings("unchecked")
    public ReservoirSampler(int sampleSize) {
        if (sampleSize <= 0) {
            throw new IllegalArgumentException("sampleSize must be greater than 1: " + sampleSize);
        }
        this.samples = (T[]) new Object[sampleSize];
        this.numSamples = sampleSize;
        this.position = 0;
        this.rand = new Random();
    }

    @SuppressWarnings("unchecked")
    public ReservoirSampler(int sampleSize, long seed) {
        this.samples = (T[]) new Object[sampleSize];
        this.numSamples = sampleSize;
        this.position = 0;
        this.rand = new Random(seed);
    }

    public ReservoirSampler(T[] samples) {
        this.samples = samples;
        this.numSamples = samples.length;
        this.position = 0;
        this.rand = new Random();
    }

    public ReservoirSampler(T[] samples, long seed) {
        this.samples = samples;
        this.numSamples = samples.length;
        this.position = 0;
        this.rand = new Random(seed);
    }

    public T[] getSample() {
        return samples;
    }

    public List<T> getSamplesAsList() {
        return Arrays.asList(samples);
    }

    public void add(T item) {
        if (item == null) {
            return;
        }
        if (position < numSamples) {// reservoir not yet full, just append
            samples[position] = item;
        } else {// find a item to replace
            int replaceIndex = rand.nextInt(position + 1);
            if (replaceIndex < numSamples) {
                samples[replaceIndex] = item;
            }
        }
        position++;
    }

    public void clear() {
        Arrays.fill(samples, null);
        this.position = 0;
    }
}
