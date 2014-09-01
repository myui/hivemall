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
package hivemall.mix.client;

import hivemall.io.PredictionModel;
import hivemall.mix.MixMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public final class MixClientHandler extends SimpleChannelInboundHandler<MixMessage> {

    private final PredictionModel model;
    private final boolean hasCovar;

    public MixClientHandler(PredictionModel model) {
        super();
        if(model == null) {
            throw new IllegalArgumentException("model is null");
        }
        this.model = model;
        this.hasCovar = model.hasCovariance();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MixMessage msg) throws Exception {
        Object feature = msg.getFeature();
        float weight = msg.getWeight();
        float covar = msg.getCovariance();
        short clock = msg.getClock();
        if(hasCovar) {
            model._set(feature, weight, clock);
        } else {
            model._set(feature, weight, covar, clock);
        }
    }

}
