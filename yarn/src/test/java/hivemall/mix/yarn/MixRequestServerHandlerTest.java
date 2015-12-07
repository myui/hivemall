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

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import io.netty.channel.ChannelHandlerContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import hivemall.mix.yarn.network.MixRequestServerHandler.MixRequestReceiver;
import hivemall.mix.yarn.network.MixRequest;
import hivemall.mix.yarn.utils.TimestampedValue;

public final class MixRequestServerHandlerTest {

    @Test
    public void testMixServerRequestReceiver() throws Exception {
        final Map<String, TimestampedValue<NodeId>> aliveMixServers = new ConcurrentHashMap<String, TimestampedValue<NodeId>>();
        aliveMixServers.put("containerId1", createNodeId("localhost", 1));
        aliveMixServers.put("containerId2", createNodeId("localhost", 2));
        aliveMixServers.put("containerId3", createNodeId("localhost", 3));

        MixRequestReceiver handler = new MixRequestReceiver(aliveMixServers);
        Method channelReadMethod = MixRequestReceiver.class.getDeclaredMethod("channelRead0", ChannelHandlerContext.class, MixRequest.class);
        channelReadMethod.setAccessible(true);
        ChannelHandlerContext mockCtx = Mockito.mock(ChannelHandlerContext.class);
        ArgumentCaptor<MixRequest> arg = ArgumentCaptor.forClass(MixRequest.class);
        channelReadMethod.invoke(handler, mockCtx, new MixRequest(1));
        Mockito.verify(mockCtx, Mockito.times(1)).writeAndFlush(Mockito.any());
        Mockito.verify(mockCtx).writeAndFlush(arg.capture());

        Assert.assertEquals(3, arg.getValue().getCount());
        final Set<String> setAllocatedURIs = parseAllocatedUris(arg.getValue().getAllocatedURIs());
        Assert.assertEquals($s("localhost:1", "localhost:2", "localhost:3"), setAllocatedURIs);
    }

    private TimestampedValue<NodeId> createNodeId(String host, int port) {
        return new TimestampedValue<NodeId>(NodeId.newInstance(host, port));
    }

    private Set<String> parseAllocatedUris(String uris) {
        final String[] urisStrArray = uris.split(Pattern.quote(MixYarnEnv.MIXSERVER_SEPARATOR));
        return new HashSet<String>(Arrays.asList(urisStrArray));
    }

    private static <T> Set<T> $s(T... values) {
        final Set<T> set = new HashSet<T>(values.length);
        Collections.addAll(set, values);
        return set;
    }
}
