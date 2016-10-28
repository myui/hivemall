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
package hivemall.utils.lock;

import java.util.concurrent.atomic.AtomicBoolean;

public final class TTASLock implements Lock {

    private final AtomicBoolean state;

    public TTASLock() {
        this(false);
    }

    public TTASLock(boolean locked) {
        this.state = new AtomicBoolean(locked);
    }

    @Override
    public void lock() {
        while (true) {
            while (state.get()); // wait until the lock free            
            if (!state.getAndSet(true)) { // now try to acquire the lock
                return;
            }
        }
    }

    @Override
    public boolean tryLock() {
        if (state.get()) {
            return false;
        }
        return !state.getAndSet(true);
    }

    @Override
    public void unlock() {
        state.set(false);
    }

    @Override
    public boolean isLocked() {
        return state.get();
    }
}
