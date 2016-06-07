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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import io.netty.handler.traffic.TrafficCounter;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public final class ThroughputCounter extends GlobalTrafficShapingHandler {
    private static final Log logger = LogFactory.getLog(ThroughputCounter.class);

    @Nonnull
    private final MixServerMetrics metrics;

    private final AtomicLong lastChacked = new AtomicLong();
    private final AtomicLong currentReads = new AtomicLong();
    private final AtomicLong currentWrites = new AtomicLong();

    private long lastReads;
    private long lastWrites;

    public ThroughputCounter(@Nonnull ScheduledExecutorService executor, long checkInterval,
            @Nonnull MixServerMetrics metrics) {
        super(executor, checkInterval);
        this.metrics = metrics;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        super.channelRead(ctx, msg);
        currentReads.incrementAndGet();
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
            throws Exception {
        super.write(ctx, msg, promise);
        currentWrites.incrementAndGet();
    }

    @Override
    protected void doAccounting(TrafficCounter counter) {
        long currentTime = System.currentTimeMillis();
        long interval = currentTime - lastChacked.getAndSet(currentTime);
        if (interval == 0) {
            return;
        }
        this.lastReads = currentReads.getAndSet(0L);
        this.lastWrites = currentWrites.getAndSet(0L);

        long readsPerSec = (lastReads / interval) * 1000;
        long writesPerSec = (lastWrites / interval) * 1000;
        metrics.setLastReads(readsPerSec);
        metrics.setLastWrites(writesPerSec);

        TrafficCounter traffic = trafficCounter();
        long readThroughput = traffic.lastReadThroughput();
        long writeThroughput = traffic.lastWriteThroughput();
        metrics.setReadThroughput(readThroughput);
        metrics.setWriteThroughput(writeThroughput);

        if (logger.isInfoEnabled()) {
            if (lastReads > 0 || lastWrites > 0) {
                logger.info(toString());
            }
        }
    }

    @Override
    public String toString() {
        TrafficCounter traffic = trafficCounter();
        final StringBuilder buf = new StringBuilder(512);
        long readThroughput = traffic.lastReadThroughput();
        buf.append("Read Throughput: ").append(readThroughput / 1024L).append(" KB/sec, ");
        buf.append(lastReads).append(" msg/sec\n");
        long writeThroughput = traffic.lastWriteThroughput();
        buf.append("Write Throughput: ").append(writeThroughput / 1024).append(" KB/sec, ");
        buf.append(lastWrites).append(" msg/sec");
        return buf.toString();
    }

}
