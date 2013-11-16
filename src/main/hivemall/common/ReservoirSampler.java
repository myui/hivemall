/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013
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
 */
package hivemall.common;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Vitter's reservoir sampler
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
        if(sampleSize <= 0) {
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
        if(item == null) {
            return;
        }
        if(position < numSamples) {// reservoir not yet full, just append
            samples[position] = item;
        } else {// find a item to replace
            int replaceIndex = rand.nextInt(position + 1);
            if(replaceIndex < numSamples) {
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