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
import hivemall.mix.store.PartialAverage;
import hivemall.mix.store.SessionStore;
import org.junit.Assert;
import org.junit.Test;

public class MixServerHandlerTest {
    static final Integer dummyFeature = 0;

    @Test
    public void MxiWeightTest() {
        SessionStore session = new SessionStore();
        MixServerHandler handler = new MixServerHandler(session, 4, 1.0f);
        PartialAverage acc = new PartialAverage();
        acc.add(1.0f, 0.0f, (short) 0, 1, 1.0f);
        MixMessage msg1 = new MixMessage(MixEventName.average, dummyFeature, 3.0f, (short) 4, 1);
        Assert.assertFalse(handler.mix(msg1, acc));
        Assert.assertEquals(1, acc.getClock());
        Assert.assertEquals(2.0, acc.getWeight(1.0f), 0.001);
        MixMessage msg2 = new MixMessage(MixEventName.average, dummyFeature, 5.0f, (short) 9, 1);
        Assert.assertTrue(handler.mix(msg2, acc));
        Assert.assertEquals(2, acc.getClock());
        Assert.assertEquals(3.0, acc.getWeight(1.0f), 0.001);
        MixMessage msg3 = new MixMessage(MixEventName.average, dummyFeature, 7.0f, (short) 1, 1);
        Assert.assertFalse(handler.mix(msg3, acc));
        Assert.assertEquals(3, acc.getClock());
        Assert.assertEquals(4.0, acc.getWeight(1.0f), 0.001);
    }
}
