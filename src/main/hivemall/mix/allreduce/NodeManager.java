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
package hivemall.mix.allreduce;

import hivemall.mix.NodeInfo;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public final class NodeManager {

    private final int maxChildConn;
    private final Map<String, LinkedList<InnerInfo>> sessionMap;

    public NodeManager(int maxChildConnPerNode) {
        if(maxChildConnPerNode <= 0) {
            throw new IllegalArgumentException("maxChildConnPerNode must be greater than 0: "
                    + maxChildConnPerNode);
        }
        this.maxChildConn = maxChildConnPerNode;
        this.sessionMap = new HashMap<String, LinkedList<InnerInfo>>();
    }

    public synchronized NodeInfo pop_push(String sessionName, NodeInfo node) {
        NodeInfo poped = pop(sessionName);
        push(sessionName, node);
        return poped;
    }

    public synchronized NodeInfo pop(String sessionName) {
        Queue<InnerInfo> nodelist = sessionMap.get(sessionName);
        if(nodelist == null) {
            return null;
        }
        InnerInfo info = nodelist.peek();
        if(info != null) {
            info.numAcceptableConnections--;
            if(info.numAcceptableConnections <= 0) {
                nodelist.remove();
                if(nodelist.isEmpty()) {
                    sessionMap.remove(sessionName);
                }
            }
        }
        return info;
    }

    public void push(String sessionName, NodeInfo node) {
        push(sessionName, node, maxChildConn);
    }

    public synchronized void push(String sessionName, NodeInfo node, int connectionNumLimit) {
        if(connectionNumLimit <= 0) {
            throw new IllegalArgumentException("connectionNumLimit must be greather than 0: "
                    + connectionNumLimit);
        }
        LinkedList<InnerInfo> nodelist = sessionMap.get(sessionName);
        if(nodelist == null) {
            nodelist = new LinkedList<NodeManager.InnerInfo>();
            sessionMap.put(sessionName, nodelist);
        }
        InnerInfo info = new InnerInfo(node, connectionNumLimit);
        nodelist.offer(info);
    }

    public synchronized void remove(String sessionName, NodeInfo node) {
        Queue<InnerInfo> nodelist = sessionMap.get(sessionName);
        if(nodelist != null) {
            if(nodelist.remove(node)) {
                if(nodelist.isEmpty()) {
                    sessionMap.remove(sessionName);
                }
            }
        }
    }

    private static final class InnerInfo extends NodeInfo {
        int numAcceptableConnections;

        InnerInfo(NodeInfo node, int numAcceptableConnections) {
            super(node);
            assert (numAcceptableConnections >= 0) : numAcceptableConnections;
            this.numAcceptableConnections = numAcceptableConnections;
        }
    }

}
