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
package hivemall.mix.server;

import hivemall.mix.MixMessage;
import hivemall.mix.MixMessage.MixEventName;
import hivemall.mix.store.PartialArgminKLD;
import hivemall.mix.store.PartialAverage;
import hivemall.mix.store.PartialResult;
import hivemall.mix.store.SessionStore;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

@Sharable
public final class MixServerHandler extends SimpleChannelInboundHandler<MixMessage> {

    @Nonnull
    private final SessionStore sessionStore;
    private final int syncThreshold;
    private final float scale;

    public MixServerHandler(@Nonnull SessionStore sessionStore, @Nonnegative int syncThreshold, @Nonnegative float scale) {
        super();
        this.sessionStore = sessionStore;
        this.syncThreshold = syncThreshold;
        this.scale = scale;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MixMessage msg) throws Exception {
        final MixEventName event = msg.getEvent();
        switch(event) {
            case average:
            case argminKLD: {
                PartialResult partial = getPartialResult(msg);
                mix(ctx, msg, partial);
                break;
            }
            case closeGroup: {
                closeGroup(msg);
                break;
            }
            default:
                throw new IllegalStateException("Unexpected event: " + event);
        }
    }

    private void closeGroup(@Nonnull MixMessage msg) {
        String groupId = msg.getGroupID();
        if(groupId == null) {
            return;
        }
        sessionStore.remove(groupId);
    }

    @Nonnull
    private PartialResult getPartialResult(@Nonnull MixMessage msg) {
        String groupID = msg.getGroupID();
        if(groupID == null) {
            throw new IllegalStateException("JobID is not set in the request message");
        }
        ConcurrentMap<Object, PartialResult> map = sessionStore.get(groupID);

        Object feature = msg.getFeature();
        PartialResult partial = map.get(feature);
        if(partial == null) {
            final MixEventName event = msg.getEvent();
            switch(event) {
                case average:
                    partial = new PartialAverage(scale);
                    break;
                case argminKLD:
                    partial = new PartialArgminKLD(scale);
                    break;
                default:
                    throw new IllegalStateException("Unexpected event: " + event);
            }
            PartialResult existing = map.putIfAbsent(feature, partial);
            if(existing != null) {
                partial = existing;
            }
        }
        return partial;
    }

    private void mix(final ChannelHandlerContext ctx, final MixMessage requestMsg, final PartialResult partial) {
        MixEventName event = requestMsg.getEvent();
        Object feature = requestMsg.getFeature();
        float weight = requestMsg.getWeight();
        float covar = requestMsg.getCovariance();
        short clock = requestMsg.getClock();
        int deltaUpdates = requestMsg.getDeltaUpdates();

        MixMessage responseMsg = null;
        try {
            partial.lock();

            int diffClock = partial.diffClock(clock);
            partial.add(weight, covar, clock, deltaUpdates);

            if(diffClock >= syncThreshold) {// sync model if clock DIFF is above threshold
                float averagedWeight = partial.getWeight();
                float minCovar = partial.getMinCovariance();
                short totalClock = partial.getClock();
                responseMsg = new MixMessage(event, feature, averagedWeight, minCovar, totalClock, 0 /* deltaUpdates */);
            }
        } finally {
            partial.unlock();
        }

        if(responseMsg != null) {
            ctx.writeAndFlush(responseMsg);
        }
    }

}
