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
package hivemall.mix.store;

import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public final class SessionObject {

    @Nonnull
    private final ConcurrentMap<Object, PartialResult> object;
    private volatile long lastAccessed; // being accessed by multiple threads

    public SessionObject(@Nonnull ConcurrentMap<Object, PartialResult> obj) {
        if(obj == null) {
            throw new IllegalArgumentException("obj is null");
        }
        this.object = obj;
    }

    @Nonnull
    public ConcurrentMap<Object, PartialResult> get() {
        return object;
    }

    /**    
     * @return last accessed time in msec
     */
    public long getLastAccessed() {
        return lastAccessed;
    }

    public void touch() {
        this.lastAccessed = System.currentTimeMillis();
    }

}
