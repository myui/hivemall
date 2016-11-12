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
package hivemall.mf;

import hivemall.utils.collections.IntOpenHashMap;
import hivemall.utils.math.MathUtils;

import java.util.Random;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public final class FactorizedModel {

    @Nonnull
    private final RatingInitilizer ratingInitializer;
    @Nonnegative
    private final int factor;

    // rank matrix initialization
    private final RankInitScheme initScheme;

    private int minIndex, maxIndex;
    @Nonnull
    private Rating meanRating;
    private IntOpenHashMap<Rating[]> users;
    private IntOpenHashMap<Rating[]> items;
    private IntOpenHashMap<Rating> userBias;
    private IntOpenHashMap<Rating> itemBias;

    private final Random[] randU, randI;

    public FactorizedModel(@Nonnull RatingInitilizer ratingInitializer, @Nonnegative int factor,
            @Nonnull RankInitScheme initScheme) {
        this(ratingInitializer, factor, 0.f, initScheme, 136861);
    }

    public FactorizedModel(@Nonnull RatingInitilizer ratingInitializer, @Nonnegative int factor,
            float meanRating, @Nonnull RankInitScheme initScheme) {
        this(ratingInitializer, factor, meanRating, initScheme, 136861);
    }

    public FactorizedModel(@Nonnull RatingInitilizer ratingInitializer, @Nonnegative int factor,
            float meanRating, @Nonnull RankInitScheme initScheme, int expectedSize) {
        this.ratingInitializer = ratingInitializer;
        this.factor = factor;
        this.initScheme = initScheme;
        this.minIndex = 0;
        this.maxIndex = 0;
        this.meanRating = ratingInitializer.newRating(meanRating);
        this.users = new IntOpenHashMap<Rating[]>(expectedSize);
        this.items = new IntOpenHashMap<Rating[]>(expectedSize);
        this.userBias = new IntOpenHashMap<Rating>(expectedSize);
        this.itemBias = new IntOpenHashMap<Rating>(expectedSize);
        this.randU = newRandoms(factor, 31L);
        this.randI = newRandoms(factor, 41L);
    }

    public enum RankInitScheme {
        random /* default */, gaussian;

        @Nonnegative
        private float maxInitValue;
        @Nonnegative
        private double initStdDev;

        @Nonnull
        public static RankInitScheme resolve(@Nullable String opt) {
            if (opt == null) {
                return random;
            } else if ("gaussian".equalsIgnoreCase(opt)) {
                return gaussian;
            } else if ("random".equalsIgnoreCase(opt)) {
                return random;
            }
            return random;
        }

        public void setMaxInitValue(float maxInitValue) {
            this.maxInitValue = maxInitValue;
        }

        public void setInitStdDev(double initStdDev) {
            this.initStdDev = initStdDev;
        }

    }

    private static Random[] newRandoms(@Nonnull final int size, final long seed) {
        final Random[] rand = new Random[size];
        for (int i = 0, len = rand.length; i < len; i++) {
            rand[i] = new Random(seed + i);
        }
        return rand;
    }

    public int getMinIndex() {
        return minIndex;
    }

    public int getMaxIndex() {
        return maxIndex;
    }

    @Nonnull
    public Rating meanRating() {
        return meanRating;
    }

    public float getMeanRating() {
        return meanRating.getWeight();
    }

    public void setMeanRating(float rating) {
        meanRating.setWeight(rating);
    }

    @Nullable
    public Rating[] getUserVector(int u) {
        return getUserVector(u, false);
    }

    @Nullable
    public Rating[] getUserVector(int u, boolean init) {
        Rating[] v = users.get(u);
        if (init && v == null) {
            v = new Rating[factor];
            switch (initScheme) {
                case random:
                    uniformFill(v, randU[0], initScheme.maxInitValue, ratingInitializer);
                    break;
                case gaussian:
                    gaussianFill(v, randU, initScheme.initStdDev, ratingInitializer);
                    break;
                default:
                    throw new IllegalStateException("Unsupported rank initialization scheme: "
                            + initScheme);

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
        if (init && v == null) {
            v = new Rating[factor];
            switch (initScheme) {
                case random:
                    uniformFill(v, randI[0], initScheme.maxInitValue, ratingInitializer);
                    break;
                case gaussian:
                    gaussianFill(v, randI, initScheme.initStdDev, ratingInitializer);
                    break;
                default:
                    throw new IllegalStateException("Unsupported rank initialization scheme: "
                            + initScheme);

            }
            items.put(i, v);
            this.maxIndex = Math.max(maxIndex, i);
            this.minIndex = Math.min(minIndex, i);
        }
        return v;
    }

    @Nonnull
    public Rating userBias(int u) {
        Rating b = userBias.get(u);
        if (b == null) {
            b = ratingInitializer.newRating(0.f); // dummy
            userBias.put(u, b);
        }
        return b;
    }

    public float getUserBias(int u) {
        Rating b = userBias.get(u);
        if (b == null) {
            return 0.f;
        }
        return b.getWeight();
    }

    public void setUserBias(int u, float value) {
        Rating b = userBias.get(u);
        if (b == null) {
            b = ratingInitializer.newRating(value);
            userBias.put(u, b);
        }
        b.setWeight(value);
    }

    @Nonnull
    public Rating itemBias(int i) {
        Rating b = itemBias.get(i);
        if (b == null) {
            b = ratingInitializer.newRating(0.f); // dummy
            itemBias.put(i, b);
        }
        return b;
    }

    @Nullable
    public Rating getItemBiasObject(int i) {
        return itemBias.get(i);
    }

    public float getItemBias(int i) {
        Rating b = itemBias.get(i);
        if (b == null) {
            return 0.f;
        }
        return b.getWeight();
    }

    public void setItemBias(int i, float value) {
        Rating b = itemBias.get(i);
        if (b == null) {
            b = ratingInitializer.newRating(value);
            itemBias.put(i, b);
        }
        b.setWeight(value);
    }

    private static void uniformFill(final Rating[] a, final Random rand, final float maxInitValue,
            final RatingInitilizer init) {
        for (int i = 0, len = a.length; i < len; i++) {
            float v = rand.nextFloat() * maxInitValue / len;
            a[i] = init.newRating(v);
        }
    }

    private static void gaussianFill(final Rating[] a, final Random[] rand, final double stddev,
            final RatingInitilizer init) {
        for (int i = 0, len = a.length; i < len; i++) {
            float v = (float) MathUtils.gaussian(0.d, stddev, rand[i]);
            a[i] = init.newRating(v);
        }
    }

}
