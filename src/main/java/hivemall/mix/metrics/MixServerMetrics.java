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
