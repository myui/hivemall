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
package hivemall.mix.client;

import hivemall.mix.MixEnv;
import hivemall.mix.MixMessage;
import hivemall.mix.NodeInfo;
import hivemall.utils.net.NetUtils;

import java.net.InetSocketAddress;

public class MixRequestRouter {
    private final String connectInfo;

    // Filled from 'connectInfo' in initialize()
    private NodeInfo[] nodes;

    public MixRequestRouter(String connectInfo) {
        this.connectInfo = connectInfo;
    }

    public void initialize() throws Exception {
        this.nodes = parseMixServerList(toMixServerList(connectInfo));
    }

    private static NodeInfo[] parseMixServerList(String connectInfo) {
         if(connectInfo == null) {
            throw new IllegalArgumentException();
        }
        String[] endpoints = connectInfo.split("\\s*,\\s*");
        final int numEndpoints = endpoints.length;
        if(numEndpoints < 1) {
            throw new IllegalArgumentException("Invalid connectInfo: " + connectInfo);
        }
        NodeInfo[] nodes = new NodeInfo[numEndpoints];
        for(int i = 0; i < numEndpoints; i++) {
            InetSocketAddress addr = NetUtils.getInetSocketAddress(endpoints[i], MixEnv.MIXSERV_DEFAULT_PORT);
            nodes[i] = new NodeInfo(addr);
        }
        return nodes;
    }

    protected String toMixServerList(String connectInfo) {
        return connectInfo;
    }

    public NodeInfo[] getAllNodes() {
        return nodes;
    }

    public NodeInfo selectNode(MixMessage msg) {
        assert (msg != null);
        Object feature = msg.getFeature();
        int hashcode = feature.hashCode();
        int index = (hashcode & Integer.MAX_VALUE) % nodes.length;
        return nodes[index];
    }

}
