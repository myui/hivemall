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

import hivemall.utils.collections.IntArrayList;
import hivemall.utils.collections.IntOpenHashMap;

import javax.annotation.Nonnull;

public class PositiveOnlyFeedback {

    @Nonnull
    protected final IntOpenHashMap<IntArrayList> rows;
    
    protected int maxItemId;
    protected int totalFeedbacks;

    public PositiveOnlyFeedback() {
        this(-1);
    }

    public PositiveOnlyFeedback(int maxItemId) {
        this.rows = new IntOpenHashMap<IntArrayList>(1024);
        this.maxItemId = maxItemId;
        this.totalFeedbacks = 0;
    }

    public int getNumUsers() {
        return rows.size();
    }

    public void setMaxItemId(int maxItemId) {
        this.maxItemId = maxItemId;
    }

    public int getMaxItemId() {
        return maxItemId;
    }

    public int getTotalFeedbacks() {
        return totalFeedbacks;
    }

    @Nonnull
    public IntArrayList getItems(final int userId, boolean nonEmptyCheck) {
        IntArrayList items = rows.get(userId);
        if (nonEmptyCheck) {
            if (items == null || items.isEmpty()) {
                throw new IllegalStateException("Found empty items for user: " + userId);
            }
        }
        return items;
    }

    public void removeFeedback(final int userId) {
        IntArrayList items = rows.remove(userId);
        if (items != null && !items.isEmpty()) {
            this.totalFeedbacks -= items.size();
        }
    }

    public void addFeedback(final int userId, @Nonnull final IntArrayList itemIds) {
        validateIndex(userId);
        if(itemIds.isEmpty()) {
            return;
        }

        IntArrayList old = rows.put(userId, itemIds);
        if (old != null) {
            throw new IllegalStateException("Entry already exists in row: " + userId);
        }

        this.totalFeedbacks += itemIds.size();
    }

    protected static void validateIndex(final int index) {
        if (index < 0) {
            throw new IllegalArgumentException("Negative index is not allowed: " + index);
        }
    }

    protected static void validateIndex(final int user, final int item) {
        if (user < 0) {
            throw new IllegalArgumentException("Negative user index is not allowed: " + user);
        }
        if (item < 0) {
            throw new IllegalArgumentException("Negative item index is not allowed: " + item);
        }
    }


}
