/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package hivemall.mix.server;

import hivemall.mix.store.PartialResult;

import org.junit.Assert;
import org.junit.Test;

public class PartialResultTest {

    @Test
    public void testOverflowUnderflowClock() {
        // The clock implementation inside PartialResult depends
        // on the overflow/underflow behavior of short-typed values
        // in the JVM specification.
        // Related discussion can be found in a link below;
        //
        // How does Java handle integer underflows and overflows and how would you check for it?
        // - http://stackoverflow.com/questions/3001836/how-does-java-handle-integer-underflows-and-overflows-and-how-would-you-check-fo
        //
        short testValue = Short.MAX_VALUE;

        Assert.assertEquals(Short.MIN_VALUE, ++testValue);
        Assert.assertEquals(Short.MAX_VALUE, --testValue);

        // Initially, clock == 0
        PartialResult value = new PartialResult() {

            @Override
            public void add(float localWeight, float covar, int deltaUpdates, float scale) {
                incrClock(deltaUpdates);
            }

            @Override
            public void subtract(float localWeight, float covar, int deltaUpdates, float scale) {}

            @Override
            public float getWeight(float scale) {
                return 0.f;
            }

            @Override
            public float getCovariance(float scale) {
                return 0.f;
            }
        };

        Assert.assertEquals(0, value.diffClock((short) 0));
        Assert.assertEquals(3, value.diffClock((short) -3));
        Assert.assertEquals(2, value.diffClock((short) 2));
        Assert.assertEquals(32767, value.diffClock(Short.MAX_VALUE));
        // Max value of PartialResult#diffClock is 32768
        Assert.assertEquals(32768, value.diffClock(Short.MIN_VALUE));
        Assert.assertEquals(32767, value.diffClock((short) (Short.MIN_VALUE - 1)));

        // Proceed the clock by 12
        value.add(0.f, 0.f, 12, 1.f);

        Assert.assertEquals(0, value.diffClock((short) 12));
        Assert.assertEquals(7, value.diffClock((short) 5));
        Assert.assertEquals(6, value.diffClock((short) 18));
        Assert.assertEquals(32767,
            value.diffClock(addWithUnderOverflow(Short.MAX_VALUE, (short) 12))); // -32757
        Assert.assertEquals(32768,
            value.diffClock(addWithUnderOverflow(Short.MAX_VALUE, (short) 13))); // -32756
        Assert.assertEquals(32767,
            value.diffClock(addWithUnderOverflow(Short.MAX_VALUE, (short) 14))); // -32755

        // Overflow test
        value.add(0.f, 0.f, Short.MAX_VALUE, 1.f);

        Assert.assertEquals(0, value.diffClock(addWithUnderOverflow(Short.MAX_VALUE, (short) 12))); // -32757
        Assert.assertEquals(2, value.diffClock(addWithUnderOverflow(Short.MAX_VALUE, (short) 10))); // -32755
        Assert.assertEquals(4, value.diffClock(addWithUnderOverflow(Short.MAX_VALUE, (short) 16))); // -32761
        Assert.assertEquals(32767, value.diffClock((short) 10));
        Assert.assertEquals(32768, value.diffClock((short) 11));
        Assert.assertEquals(32767, value.diffClock((short) 12));
    }

    @Test
    public void testBoundaries() {
        Partial partial = new Partial();

        partial.setGlobalClock(Short.MAX_VALUE);
        Assert.assertEquals(0, partial.diffClock(Short.MAX_VALUE));
        Assert.assertEquals(1, partial.diffClock(Short.MIN_VALUE));

        partial.setGlobalClock((short) (Short.MAX_VALUE + 1));
        Assert.assertEquals(Short.MIN_VALUE, partial.getClock());

        partial.setGlobalClock((short) (Short.MAX_VALUE - 10));
        Assert.assertEquals(21, partial.diffClock((short) (Short.MIN_VALUE + 10)));
    }

    public void testDiff() {
        Partial partial = new Partial();

        partial.setGlobalClock((short) 2);
        Assert.assertEquals(2, partial.diffClock((short) 0));
        Assert.assertEquals(4, partial.diffClock((short) -2));

        partial.setGlobalClock((short) -2);
        Assert.assertEquals(2, partial.diffClock((short) 0));
        Assert.assertEquals(0, partial.diffClock((short) -2));
        Assert.assertEquals(4, partial.diffClock((short) 2));
    }

    private static short addWithUnderOverflow(short value1, short value2) {
        value1 += value2;
        return value1;
    }

    private static class Partial extends PartialResult {

        public void setGlobalClock(short global) {
            this.globalClock = global;
        }

        @Override
        public void add(float localWeight, float covar, int deltaUpdates, float scale) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void subtract(float localWeight, float covar, int deltaUpdates, float scale) {
            throw new UnsupportedOperationException();
        }

        @Override
        public float getWeight(float scale) {
            throw new UnsupportedOperationException();
        }

        @Override
        public float getCovariance(float scale) {
            throw new UnsupportedOperationException();
        }

    }

}
