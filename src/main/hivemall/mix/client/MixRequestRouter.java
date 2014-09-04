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

import hivemall.mix.MixMessage;
import hivemall.mix.NodeInfo;
import hivemall.mix.server.MixServer;
import hivemall.utils.net.NetUtils;

import java.net.InetSocketAddress;

public final class MixRequestRouter {

    private final int numNodes;
    private final NodeInfo[] nodes;

    public MixRequestRouter(String connectInfo) {
        if(connectInfo == null) {
            throw new IllegalArgumentException();
        }
        String[] endpoints = connectInfo.split("\\s*,\\s*");
        final int numEndpoints = endpoints.length;
        if(numEndpoints < 1) {
            throw new IllegalArgumentException("Invalid connectInfo: " + connectInfo);
        }
        this.numNodes = numEndpoints;
        NodeInfo[] nodes = new NodeInfo[numEndpoints];
        for(int i = 0; i < numEndpoints; i++) {
            InetSocketAddress addr = NetUtils.getInetSocketAddress(endpoints[i], MixServer.DEFAULT_PORT);
            nodes[i] = new NodeInfo(addr);
        }
        this.nodes = nodes;
    }

    public NodeInfo[] getAllNodes() {
        return nodes;
    }

    public NodeInfo selectNode(MixMessage msg) {
        assert (msg != null);
        Object feature = msg.getFeature();
        int hashcode = feature.hashCode();
        int index = Math.abs(hashcode) % numNodes;
        return nodes[index];
    }

}
