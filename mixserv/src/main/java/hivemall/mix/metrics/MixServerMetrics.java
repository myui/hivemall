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
package hivemall.mix.metrics;

public final class MixServerMetrics implements MixServerMetricsMBean {

    private long readThroughput;
    private long writeThroughput;
    private long lastReads;
    private long lastWrites;

    public MixServerMetrics() {}

    public void setReadThroughput(long readThroughput) {
        this.readThroughput = readThroughput;
    }

    public void setWriteThroughput(long writeThroughput) {
        this.writeThroughput = writeThroughput;
    }

    public void setLastReads(long lastReads) {
        this.lastReads = lastReads;
    }

    public void setLastWrites(long lastWrites) {
        this.lastWrites = lastWrites;
    }

    @Override
    public long getReadThroughput() {
        return readThroughput;
    }

    @Override
    public long getWriteThroughput() {
        return writeThroughput;
    }

    @Override
    public long getLastReads() {
        return lastReads;
    }

    @Override
    public long getLastWrites() {
        return lastWrites;
    }

}
