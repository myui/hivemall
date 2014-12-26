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
 */
package hivemall.io;

import hivemall.utils.collections.IntOpenHashMap;

import java.util.Random;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public final class FactorizedModel {

    private final int factor;
    private final boolean randInit;

    private int minIndex, maxIndex;
    private float meanRating;
    private IntOpenHashMap<Rating[]> users;
    private IntOpenHashMap<Rating[]> items;
    private SparseFloatVector userBias;
    private SparseFloatVector itemBias;

    private final Random randU, randI;

    public FactorizedModel(int factor, float meanRating, boolean randInit, int expectedSize) {
        this.factor = factor;
        this.minIndex = 0;
        this.maxIndex = 0;
        this.meanRating = meanRating;
        this.users = new IntOpenHashMap<Rating[]>(expectedSize);
        this.items = new IntOpenHashMap<Rating[]>(expectedSize);
        this.userBias = new SparseFloatVector(expectedSize);
        this.itemBias = new SparseFloatVector(expectedSize);
        this.randInit = randInit;
        this.randU = new Random(31L);
        this.randI = new Random(41L);
    }

    public int getMinIndex() {
        return minIndex;
    }

    public int getMaxIndex() {
        return maxIndex;
    }

    public float getMeanRating() {
        return meanRating;
    }

    public void setMeanRating(float rating) {
        this.meanRating = rating;
    }

    @Nullable
    public Rating[] getUserVector(int u) {
        return getUserVector(u, false);
    }

    @Nullable
    public Rating[] getUserVector(int u, boolean init) {
        Rating[] v = users.get(u);
        if(init && v == null) {
            v = new Rating[factor];
            if(randInit) {
                fill(v, randU);
            }
            users.put(u, v);
            this.maxIndex = Math.max(maxIndex, u);
            this.minIndex = Math.min(minIndex, u);
        }
        return v;
    }

    @Nullable
    public Rating[] getItemVector(int i) {
        return getItemVector(i, false);
    }

    @Nullable
    public Rating[] getItemVector(int i, boolean init) {
        Rating[] v = items.get(i);
        if(init && v == null) {
            v = new Rating[factor];
            if(randInit) {
                fill(v, randI);
            }
            items.put(i, v);
            this.maxIndex = Math.max(maxIndex, i);
            this.minIndex = Math.min(minIndex, i);
        }
        return v;
    }

    public float getUserBias(int u) {
        return userBias.get(u);
    }

    public void setUserBias(int u, float value) {
        userBias.set(u, value);
    }

    public float getItemBias(int i) {
        return itemBias.get(i);
    }

    public void setItemBias(int i, float value) {
        itemBias.set(i, value);
    }

    private static void fill(final Rating[] a, final Random rand) {
        for(int i = 0, len = a.length; i < len; i++) {
            float v = rand.nextFloat();
            a[i] = new Rating(v);
        }
    }
}
