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

import static org.mockito.Mockito.mock;
import hivemall.mix.MixMessage;
import hivemall.mix.MixMessage.MixEventName;
import hivemall.mix.store.PartialAverage;
import hivemall.mix.store.PartialResult;
import hivemall.mix.store.SessionObject;
import hivemall.mix.store.SessionStore;
import hivemall.test.HivemallTestBase;
import io.netty.channel.ChannelHandlerContext;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public final class MixServerHandlerTest extends HivemallTestBase {

    static final Integer dummyFeature = 0;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void MxiWeightTest() throws NoSuchMethodException, SecurityException,
            IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        SessionStore session = new SessionStore();
        MixServerHandler handler = new MixServerHandler(session, 4, 1.0f);

        Method mixMethod = MixServerHandler.class.getDeclaredMethod("mix", ChannelHandlerContext.class, MixMessage.class, PartialResult.class, SessionObject.class);
        mixMethod.setAccessible(true);

        SessionObject sessionObj = session.get("dummy");

        // Initially, clock=0
        PartialAverage acc = new PartialAverage();
        MixMessage msg1 = new MixMessage(MixEventName.average, dummyFeature, 3.0f, (short) 5, 1);

        mixMethod.invoke(handler, ctx, msg1, acc, sessionObj);
        Assert.assertEquals(1, acc.getClock());
        Assert.assertEquals(3.0, acc.getWeight(1.0f), 0.001);

        MixMessage msg2 = new MixMessage(MixEventName.average, dummyFeature, 5.0f, (short) -1, 1);
        mixMethod.invoke(handler, ctx, msg2, acc, sessionObj);
        Assert.assertEquals(2, acc.getClock());
        Assert.assertEquals(4.0, acc.getWeight(1.0f), 0.001);

        MixMessage msg3 = new MixMessage(MixEventName.average, dummyFeature, 7.0f, (short) 6, 1);
        mixMethod.invoke(handler, ctx, msg3, acc, sessionObj);
        Assert.assertEquals(3, acc.getClock());
        Assert.assertEquals(5.0, acc.getWeight(1.0f), 0.001);

        // Check expected exceptions
        exception.expectCause(new CauseMatcher(IllegalArgumentException.class, "Illegal deltaUpdates received: 0"));
        MixMessage msg4 = new MixMessage(MixEventName.average, dummyFeature, 0.0f, (short) 0, 0);
        mixMethod.invoke(handler, ctx, msg4, acc, sessionObj);
    }

    private static class CauseMatcher extends TypeSafeMatcher<Throwable> {

        private final Class<? extends Throwable> type;
        private final String expectedMessage;

        public CauseMatcher(Class<? extends Throwable> type, String expectedMessage) {
            this.type = type;
            this.expectedMessage = expectedMessage;
        }

        @Override
        protected boolean matchesSafely(Throwable item) {
            return item.getClass().isAssignableFrom(type)
                    && item.getMessage().contains(expectedMessage);
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("expects type ").appendValue(type).appendText(" and a message ").appendValue(expectedMessage);
        }
    }

}
