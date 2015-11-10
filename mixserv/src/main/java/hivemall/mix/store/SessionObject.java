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
package hivemall.mix.store;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public final class SessionObject {

    @Nonnull
    private final ConcurrentMap<Object, PartialResult> object;
    private volatile long lastAccessed; // being accessed by multiple threads

    private final AtomicLong numRequests;
    private final AtomicLong numResponses;

    public SessionObject(@Nonnull ConcurrentMap<Object, PartialResult> obj) {
        if(obj == null) {
            throw new IllegalArgumentException("obj is null");
        }
        this.object = obj;
        this.numRequests = new AtomicLong(0L);
        this.numResponses = new AtomicLong(0L);
    }

    @Nonnull
    public ConcurrentMap<Object, PartialResult> get() {
        return object;
    }

    /**    
     * @return last accessed time in msec
     */
    public long getLastAccessed() {
        return lastAccessed;
    }

    public void incrRequest() {
        this.lastAccessed = System.currentTimeMillis();
        numRequests.getAndIncrement();
    }

    public void incrResponse() {
        numResponses.getAndIncrement();
    }

    public long getRequests() {
        return numRequests.get();
    }

    public long getResponses() {
        return numResponses.get();
    }

    public String getSessionInfo() {
        long requests = numRequests.get();
        long responses = numResponses.get();
        float percentage = ((float) ((double) responses / requests)) * 100.f;
        return "#requests: " + requests + ", #responses: " + responses + " ("
                + String.format("%,.2f", percentage) + "%)";
    }

}
