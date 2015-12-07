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
package hivemall.mix.yarn;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import io.netty.channel.ChannelHandlerContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import hivemall.mix.yarn.network.Heartbeat;
import hivemall.mix.yarn.network.HeartbeatHandler;
import hivemall.mix.yarn.utils.TimestampedValue;

public final class HeartbeatHandlerTest {

    @Test
    public void testHeartbeatReceiver() throws Exception {
        final ConcurrentHashMap<String, TimestampedValue<NodeId>> aliveMixServers = new ConcurrentHashMap<String, TimestampedValue<NodeId>>();
        aliveMixServers.put("containerId1", createNodeId("localhost", -1)); // -1 means an inactive entry
        aliveMixServers.put("containerId2", createNodeId("localhost", -1));

        ChannelHandlerContext mockCtx = Mockito.mock(ChannelHandlerContext.class);
        HeartbeatHandler.HeartbeatReceiver handler = new HeartbeatHandler.HeartbeatReceiver(aliveMixServers);
        handler.channelRead0(mockCtx, new Heartbeat("containerId1", "localhost", 1));
        Mockito.verify(mockCtx, Mockito.times(0)).writeAndFlush(Mockito.any());
        Assert.assertEquals($s("localhost:1", "localhost:-1"), getMapMixServers(aliveMixServers));
    }

    private TimestampedValue<NodeId> createNodeId(String host, int port) {
        return new TimestampedValue<NodeId>(NodeId.newInstance(host, port));
    }

    private Set<String> getMapMixServers(Map<String, TimestampedValue<NodeId>> data) {
        final Set<String> result = new HashSet<String>();
        for(TimestampedValue<NodeId> node : data.values()) {
            result.add(node.getValue().toString());
        }
        return result;
    }

    private static <T> Set<T> $s(T... values) {
        final Set<T> set = new HashSet<T>(values.length);
        Collections.addAll(set, values);
        return set;
    }
}
