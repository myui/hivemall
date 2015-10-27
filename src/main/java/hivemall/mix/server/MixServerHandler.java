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
package hivemall.mix.server;

import hivemall.mix.MixMessage;
import hivemall.mix.MixMessage.MixEventName;
import hivemall.mix.store.PartialArgminKLD;
import hivemall.mix.store.PartialAverage;
import hivemall.mix.store.PartialResult;
import hivemall.mix.store.SessionObject;
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

    public MixServerHandler(
            @Nonnull SessionStore sessionStore,
            @Nonnegative int syncThreshold,
            @Nonnegative float scale) {
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
                SessionObject session = getSession(msg);
                PartialResult partial = getPartialResult(msg, session);
                if (mix(msg, partial)) {
                    try {
                        partial.lock();

                        // Replace the local weight
                        ctx.writeAndFlush(new MixMessage(
                                event, msg.getFeature(), partial.getWeight(scale),
                                partial.getCovariance(scale),
                                partial.getClock(),
                                0) // deltaUpdate
                            );

                        session.incrResponse();
                    } finally {
                        partial.unlock();
                    }
                }
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
    private SessionObject getSession(@Nonnull MixMessage msg) {
        String groupID = msg.getGroupID();
        if(groupID == null) {
            throw new IllegalStateException("JobID is not set in the request message");
        }
        SessionObject session = sessionStore.get(groupID);
        session.incrRequest();
        return session;
    }

    @Nonnull
    private PartialResult getPartialResult(
            @Nonnull MixMessage msg,
            @Nonnull SessionObject session) {
        final ConcurrentMap<Object, PartialResult> map = session.get();

        Object feature = msg.getFeature();
        PartialResult partial = map.get(feature);
        if(partial == null) {
            final MixEventName event = msg.getEvent();
            switch(event) {
                case average:
                    partial = new PartialAverage();
                    break;
                case argminKLD:
                    partial = new PartialArgminKLD();
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

    /**
     * Mix incoming weights with accumulated values.
     *
     * @param requestMsg message from multiple learning models
     * @param partial accumulator for each feature
     * @return true if sync needed; otherwise false
     */
    public boolean mix(final MixMessage requestMsg, final PartialResult partial) {
        final float weight = requestMsg.getWeight();
        final float covar = requestMsg.getCovariance();
        final short clock = requestMsg.getClock();
        final int deltaUpdates = requestMsg.getDeltaUpdates();
        final boolean cancelRequest = requestMsg.isCancelRequest();

        int clockDiff = 0;
        try {
            partial.lock();

            if (cancelRequest) {
                partial.subtract(weight, covar, deltaUpdates, scale);
            } else {
                clockDiff = partial.diffClock(clock);
                // Update the weight
                partial.add(weight, covar, (short) deltaUpdates,
                        deltaUpdates, scale);
            }
        } finally {
            partial.unlock();
        }
        return Math.abs(clockDiff) > syncThreshold;
    }
}
