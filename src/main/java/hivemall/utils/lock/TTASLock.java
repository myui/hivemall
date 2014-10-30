/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013-2014
 *   National Institute of Advanced Industrial Science and Technology (AIST)
 *   Registration Number: H25PRO-1520
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
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
        while(true) {
            while(state.get())
                ; // wait until the lock free            
            if(!state.getAndSet(true)) { // now try to acquire the lock
                return;
            }
        }
    }

    @Override
    public boolean tryLock() {
        if(state.get()) {
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
