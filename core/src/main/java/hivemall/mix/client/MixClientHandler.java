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
package hivemall.mix.client;

import hivemall.mix.MixMessage;
import hivemall.mix.MixedModel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

@Sharable
public final class MixClientHandler extends SimpleChannelInboundHandler<MixMessage> {

    private final MixedModel model;

    public MixClientHandler(MixedModel model) {
        super();
        if (model == null) {
            throw new IllegalArgumentException("model is null");
        }
        this.model = model;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MixMessage msg) throws Exception {
        Object feature = msg.getFeature();
        float weight = msg.getWeight();
        short clock = msg.getClock();
        float covar = msg.getCovariance();
        model.set(feature, weight, covar, clock);
    }

}
