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

import hivemall.mix.store.PartialResult;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnegative;

public class PartialResultTest {

    private class TestPartialResult extends PartialResult {

        @Override
        public void add(
                float localWeight, float covar, short clock,
                @Nonnegative int deltaUpdates,
                float scale) {}

        @Override
        public void subtract(
                float localWeight, float covar,
                @Nonnegative int deltaUpdates,
                float scale) {}

        @Override
        public float getWeight(float scale) {
            return 0;
        }

        @Override
        public float getCovariance(float scale) {
            return 0;
        }
    }

    @Test
    public void OverflowUnderflowClockTest() {
        // The clock implementation inside PartialResult depends
        // on the overflow/underflow beauvoir of short-typed values
        // in the JVM specification.
        // Related discussion can be found in a link below;
        //
        // How does Java handle integer underflows and overflows and how would you check for it?
        // - http://stackoverflow.com/questions/3001836/how-does-java-handle-integer-underflows-and-overflows-and-how-would-you-check-fo
        //
        short testValue = Short.MAX_VALUE;

        Assert.assertEquals(Short.MIN_VALUE, ++testValue);
        Assert.assertEquals(Short.MAX_VALUE, --testValue);

        TestPartialResult value1 = new TestPartialResult();

        // Simple test
        Assert.assertEquals(0, value1.diffClock((short) 0));
        Assert.assertEquals(3, value1.diffClock((short) 3));
        Assert.assertEquals(-2, value1.diffClock((short) -2));

        // Freeze clock test
        Assert.assertEquals(-PartialResult.FREEZE_CLOCK_GAP,
                value1.diffClock((short) -PartialResult.FREEZE_CLOCK_GAP));
        Assert.assertEquals(57342,
                value1.diffClock((short) -(PartialResult.FREEZE_CLOCK_GAP + 1)));

        // Proceed the clock
        value1.incrClock((short) 12);

        Assert.assertEquals(6, value1.diffClock((short) 18));
        Assert.assertEquals(0, value1.diffClock((short) 12));
        Assert.assertEquals(-7, value1.diffClock((short) 5));

        // Overflow test
        TestPartialResult value2 = new TestPartialResult();

        for (int i = 0; i <= Short.MAX_VALUE; i++) {
            value2.incrClock((short) 1);
        }

        Assert.assertEquals(0, value2.diffClock(Short.MIN_VALUE));
        Assert.assertEquals(1, value2.diffClock((short) (Short.MIN_VALUE + 1)));
        Assert.assertEquals(-1, value2.diffClock(Short.MAX_VALUE));
    }
}
