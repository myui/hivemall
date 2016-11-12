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
package hivemall.utils.concurrent;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public final class NamedThreadFactory implements ThreadFactory {

    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final ThreadGroup group;
    private final String namePrefix;

    private boolean daemon = false;
    private int threadPriority = Thread.NORM_PRIORITY;

    public NamedThreadFactory(String threadName) {
        this(threadName, false);
    }

    public NamedThreadFactory(String threadName, boolean daemon) {
        if (threadName == null) {
            throw new IllegalArgumentException();
        }
        SecurityManager s = System.getSecurityManager();
        this.group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        this.namePrefix = threadName + '-';
        this.daemon = daemon;
    }

    public NamedThreadFactory(String threadName, ThreadGroup threadGroup) {
        if (threadName == null) {
            throw new IllegalArgumentException();
        }
        if (threadGroup == null) {
            throw new IllegalArgumentException();
        }
        this.group = threadGroup;
        this.namePrefix = threadName + '-';
    }

    public void setDaemon(boolean daemon) {
        this.daemon = daemon;
    }

    public void setPriority(int priority) {
        this.threadPriority = priority;
    }

    public Thread newThread(Runnable r) {
        final Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
        if (t.isDaemon() != daemon) {
            t.setDaemon(daemon);
        }
        if (t.getPriority() != threadPriority) {
            t.setPriority(threadPriority);
        }
        return t;
    }

}
