package hivemall.mix.metrics;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import io.netty.handler.traffic.TrafficCounter;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

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

public final class ThroughputCounter extends GlobalTrafficShapingHandler {

    @Nonnull
    private final MixServerMetrics metrics;

    private final AtomicLong lastChacked = new AtomicLong();
    private final AtomicLong currentReads = new AtomicLong();
    private final AtomicLong currentWrites = new AtomicLong();

    private long lastReads;
    private long lastWrites;

    public ThroughputCounter(@Nonnull ScheduledExecutorService executor, long checkInterval, @Nonnull MixServerMetrics metrics) {
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
        super.doAccounting(counter);

        long currentTime = System.currentTimeMillis();
        long interval = currentTime - lastChacked.getAndSet(currentTime);
        if(interval == 0) {
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
    }

    @Override
    public String toString() {
        TrafficCounter traffic = trafficCounter();
        final StringBuilder buf = new StringBuilder(512);
        long readThroughput = traffic.lastReadThroughput();
        buf.append("Read Throughput: ").append(readThroughput / 1048576L).append(" MB/sec, ");
        buf.append(lastReads).append(" msg/sec\n");
        long writeThroughput = traffic.lastWriteThroughput();
        buf.append("Write Throughput: ").append(writeThroughput / 1048576L).append(" MB/sec\n");
        buf.append(lastWrites).append(" msg/sec\n");
        return buf.toString();
    }

}
