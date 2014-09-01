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
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Sharable
public final class MixServerHandler extends SimpleChannelInboundHandler<MixMessage> {
    private static final short CLOCK_ZERO = 0;
    private static final int EXPECTED_MODEL_SIZE = 16777217; /* 2^24+1=16777216+1=16777217 */

    private final int syncThreshold;
    private final float scale;

    private final ConcurrentMap<String, ConcurrentMap<Object, PartialResult>> groupMap;

    public MixServerHandler(int syncThreshold, float scale) {
        super();
        this.syncThreshold = syncThreshold;
        this.scale = scale;
        this.groupMap = new ConcurrentHashMap<String, ConcurrentMap<Object, PartialResult>>();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MixMessage msg) throws Exception {
        final MixEventName event = msg.getEvent();
        switch(event) {
            case average:
            case argminKLD:
                PartialResult partial = getPartialResult(msg);
                mix(ctx, msg, partial);
                break;
            default:
                throw new IllegalStateException("Unexpected event: " + event);
        }
    }

    private PartialResult getPartialResult(MixMessage msg) {
        String groupID = msg.getGroupID();
        if(groupID == null) {
            throw new IllegalStateException("JobID is not set in the request message");
        }
        ConcurrentMap<Object, PartialResult> map = groupMap.get(groupID);
        if(map == null) {
            map = new ConcurrentHashMap<Object, PartialResult>(EXPECTED_MODEL_SIZE);
            ConcurrentMap<Object, PartialResult> existing = groupMap.putIfAbsent(groupID, map);
            if(existing != null) {
                map = existing;
            }
        }

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

        MixMessage responseMsg = null;
        try {
            partial.lock();

            int diffClock = partial.diffClock(clock);
            partial.add(weight, covar, clock);

            if(diffClock >= syncThreshold) {// sync model if clock DIFF is above threshold
                float averagedWeight = partial.getWeight();
                float minCovar = partial.getMinCovariance();
                responseMsg = new MixMessage(event, feature, averagedWeight, minCovar, CLOCK_ZERO);
            }
        } finally {
            partial.unlock();
        }

        if(responseMsg != null) {
            ctx.writeAndFlush(responseMsg);
        }
    }

}
