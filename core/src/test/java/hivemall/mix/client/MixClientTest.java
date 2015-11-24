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

import hivemall.io.DenseModel;
import hivemall.mix.MixMessage.MixEventName;
import hivemall.mix.MixedModel;
import hivemall.mix.client.router.MixRequestHashRouter;
import hivemall.mix.client.router.MixRequestRouter;

import java.lang.reflect.Field;

import org.junit.Assert;
import org.junit.Test;

public class MixClientTest {
    private static final MixedModel dummyModel = new DenseModel(1024);

    @Test
    public void testMixClient() throws Exception {
        Assert.assertTrue(createMixRouter("yarn://127.0.0.1").getClass().isAssignableFrom(
            MixRequestHashRouter.class));
        Assert.assertTrue(createMixRouter("yarn://localhost").getClass().isAssignableFrom(
            MixRequestHashRouter.class));
        Assert.assertTrue(createMixRouter("127.0.0.1").getClass().isAssignableFrom(
            MixRequestHashRouter.class));
        Assert.assertTrue(createMixRouter("localhost").getClass().isAssignableFrom(
            MixRequestHashRouter.class));
    }

    static MixRequestRouter createMixRouter(String connectInfo) throws Exception {
        MixClient client = new MixClient(MixEventName.average, "dummyId", connectInfo, false, 3,
            dummyModel);
        Field mixYarnRouterField = MixClient.class.getDeclaredField("router");
        mixYarnRouterField.setAccessible(true);
        return (MixRequestRouter) mixYarnRouterField.get(client);
    }

}
