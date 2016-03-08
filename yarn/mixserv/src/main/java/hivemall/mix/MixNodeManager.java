/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
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
package hivemall.mix;

import hivemall.mix.api.NodeId;
import hivemall.mix.api.NodeState;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

public final class MixNodeManager {
    public static final String ENV_MIX_COORDINATOR_ENDPOINT = "MIX_COORDINATOR_ENDPOINT";

    private final ReadWriteLock lock;
    @GuardedBy("lock")
    private final List<NodeId> activeNodes;
    @GuardedBy("lock")
    private final List<NodeId> suspectedNodes;
    @GuardedBy("lock")
    private final List<NodeId> droppedNodes;
    @GuardedBy("lock")
    private final Map<NodeId, NodeState> nodeStates;

    public MixNodeManager() {
        this.lock = new ReentrantReadWriteLock();
        this.activeNodes = new ArrayList<NodeId>();
        this.suspectedNodes = new ArrayList<NodeId>();
        this.droppedNodes = new ArrayList<NodeId>();
        this.nodeStates = new HashMap<NodeId, NodeState>();
    }

    public void join(@Nonnull NodeId node) {
        synchronized (lock) {
            activeNodes.add(node);
            nodeStates.put(node, NodeState.ACTIVE);
        }
    }
    
    

}
