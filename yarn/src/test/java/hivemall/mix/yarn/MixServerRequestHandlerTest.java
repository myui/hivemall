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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import hivemall.mix.yarn.network.MixServerRequest;
import io.netty.channel.ChannelHandlerContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.junit.Assert;
import org.junit.Test;

import hivemall.mix.yarn.network.MixServerRequestHandler.MixServerRequestReceiver;
import hivemall.mix.yarn.utils.TimestampedValue;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public final class MixServerRequestHandlerTest {

    @Test
    public void testMixServerRequestReceiver() throws Exception {
        final Map<String, TimestampedValue<NodeId>> aliveMixServers = new ConcurrentHashMap<String, TimestampedValue<NodeId>>();
        aliveMixServers.put("containerId1", createNodeId("localhost", 0));
        aliveMixServers.put("containerId2", createNodeId("localhost", 1));
        aliveMixServers.put("containerId3", createNodeId("localhost", 2));
        aliveMixServers.put("containerId4", createNodeId("localhost", 3));

        ChannelHandlerContext mockCtx = Mockito.mock(ChannelHandlerContext.class);
        ArgumentCaptor<MixServerRequest> arg = ArgumentCaptor.forClass(MixServerRequest.class);
        MixServerRequestReceiver handler = new MixServerRequestReceiver(aliveMixServers);
        handler.channelRead0(mockCtx, new MixServerRequest(1));
        Mockito.verify(mockCtx).writeAndFlush(arg.capture());

        Assert.assertEquals(4, arg.getValue().getCount());
        final Set<String> setAllocatedURIs = parseAllocatedUris(arg.getValue().getAllocatedURIs());
        final Set<String> expectedSet = new HashSet<String>();
        expectedSet.add("localhost:0");
        expectedSet.add("localhost:1");
        expectedSet.add("localhost:2");
        expectedSet.add("localhost:3");
        Assert.assertEquals(expectedSet, setAllocatedURIs);
    }

    private TimestampedValue<NodeId> createNodeId(String host, int port) {
        return new TimestampedValue<NodeId>(NodeId.newInstance(host, port));
    }

    private Set<String> parseAllocatedUris(String uris) {
        final String[] urisStrArray = uris.split(Pattern.quote(MixYarnEnv.MIXSERVER_SEPARATOR));
        return new HashSet<String>(Arrays.asList(urisStrArray));
    }
}
