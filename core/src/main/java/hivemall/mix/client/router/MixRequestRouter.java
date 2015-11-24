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
package hivemall.mix.client.router;

import hivemall.mix.MixEnv;
import hivemall.mix.MixException;
import hivemall.mix.MixMessage;
import hivemall.mix.NodeInfo;
import hivemall.utils.net.NetUtils;

import java.net.InetSocketAddress;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public abstract class MixRequestRouter {

    @Nonnull
    protected final String connectInfo;

    @Nullable
    protected NodeInfo[] nodes;

    public MixRequestRouter(@CheckForNull String connectInfo) {
        if (connectInfo == null) {
            throw new IllegalArgumentException("connectInfo is null");
        }
        this.connectInfo = connectInfo;
    }

    public void initialize() throws MixException {
        if (connectInfo.startsWith("yarn://")) {
            String endpointUrl = connectInfo.replace("yarn://", "").trim();
            // TODO
            throw new MixException("yarn:// endpoint is not yet supported: " + endpointUrl);
        } else {
            this.nodes = parseMixServerList(connectInfo);
        }
    }

    @Nonnull
    public NodeInfo[] getAllNodes() {
        return nodes;
    }

    @Nonnull
    public abstract NodeInfo selectNode(@Nonnull MixMessage msg) throws MixException;

    private static NodeInfo[] parseMixServerList(@Nonnull String connectInfo) throws MixException {
        String[] endpoints = connectInfo.split("\\s*,\\s*");
        final int numEndpoints = endpoints.length;
        if (numEndpoints < 1) {
            throw new MixException("Invalid connectInfo: " + connectInfo);
        }
        final NodeInfo[] nodes = new NodeInfo[numEndpoints];
        for (int i = 0; i < numEndpoints; i++) {
            InetSocketAddress addr = NetUtils.getInetSocketAddress(endpoints[i],
                MixEnv.MIXSERV_DEFAULT_PORT);
            nodes[i] = new NodeInfo(addr);
        }
        return nodes;
    }
}
