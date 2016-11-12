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
package hivemall.ftvec.ranking;

import hivemall.utils.collections.IntArrayList;
import hivemall.utils.lang.ArrayUtils;

import java.util.Random;

import javax.annotation.Nonnull;

public final class PerEventPositiveOnlyFeedback extends PositiveOnlyFeedback {

    @Nonnull
    private final IntArrayList users;
    @Nonnull
    private final IntArrayList posItems;

    public PerEventPositiveOnlyFeedback(int maxItemId) {
        super(maxItemId);
        this.users = new IntArrayList(1024);
        this.posItems = new IntArrayList(1024);
        this.totalFeedbacks = 0;
    }

    @Nonnull
    public int[] getRandomIndex(@Nonnull final Random rand) {
        final int[] index = new int[totalFeedbacks];
        for (int i = 0; i < index.length; i++) {
            index[i] = i;
        }
        ArrayUtils.shuffle(index, rand);
        return index;
    }

    public int getUser(final int index) {
        return users.fastGet(index);
    }

    public int getPositiveItem(final int index) {
        return posItems.fastGet(index);
    }

    @Override
    public void addFeedback(int userId, @Nonnull final IntArrayList itemIds) {
        super.addFeedback(userId, itemIds);

        for (int i = 0, size = itemIds.size(); i < size; i++) {
            int posItem = itemIds.fastGet(i);
            users.add(userId);
            posItems.add(posItem);
        }
    }

    @Override
    public void removeFeedback(int userId) {
        throw new UnsupportedOperationException();
    }

}
