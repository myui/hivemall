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
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This code is based on https://github.com/y-tag/java-Hadoop-AdditionalNetwork
 */
package hivemall.mix.allreduce;

import hivemall.mix.ConnectionInfo;
import hivemall.mix.NodeInfo;
import hivemall.mix.allreduce.AllReduceCoordinator.Command;
import hivemall.utils.io.IOUtils;
import hivemall.utils.net.NetUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public final class AllReduceWorker implements Runnable {
    private static final Log logger = LogFactory.getLog(AllReduceWorker.class);
    private static final int TIMEOUT_IN_MILLIS = 30 * 1000; // 30 seconds

    private final AllReduceContext context;
    private final ServerSocket serverSocket;
    private final NodeInfo localNode;

    private final AtomicBoolean terminate;

    public AllReduceWorker(AllReduceContext context) {
        this.context = context;
        try {
            this.serverSocket = new ServerSocket(0);
            serverSocket.setSoTimeout(TIMEOUT_IN_MILLIS);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        this.localNode = new NodeInfo(serverSocket);
        this.terminate = new AtomicBoolean(false);
    }

    public AllReduceContext getContext() {
        return context;
    }

    /**
     * Accepts child connections
     */
    @Override
    public void run() {
        while(!terminate.get()) {
            try {
                Socket socket = serverSocket.accept();
                ConnectionInfo childConn = new ConnectionInfo(socket);
                if(logger.isInfoEnabled()) {
                    logger.info("Accepted a child connection: " + childConn);
                }
                context.addChildConnection(childConn);
            } catch (SocketTimeoutException ste) {
                if(logger.isDebugEnabled()) {
                    logger.debug("Socket timeouted");
                }
                continue;
            } catch (IOException ioe) {
                logger.warn(ioe);
                continue;
            }
        }
        IOUtils.closeQuietly(serverSocket);
    }

    /**
     * Register this node to the coordinator and configure the parent
     */
    public void registerNode() throws IOException {
        final String sessionName = context.getSessionName();
        final Socket sock = connectToCoordinator();
        final DataInputStream in = NetUtils.getDataInputStream(sock);
        final DataOutputStream out = NetUtils.getDataOutputStream(sock);
        final NodeInfo parent;
        try {
            // 1. write a command
            IOUtils.writeEnum(Command.registerNode, out);
            // 2. write a session name
            IOUtils.writeString(sessionName, out);
            // 3. write the local node
            NodeInfo.writeTo(localNode, out);
            out.flush();
            // 4. read a parent node
            parent = NodeInfo.readFrom(in);
        } finally {
            IOUtils.closeQuietly(sock);
        }
        if(logger.isInfoEnabled()) {
            logger.info("Configured the parent: " + parent);
        }
        if(parent != null) {
            // connect to the parent
            SocketAddress parentEndpoint = parent.getSocketAddress();
            Socket parentSock = NetUtils.openSocket(parentEndpoint);
            ConnectionInfo parentConn = new ConnectionInfo(parentSock);
            context.setParentConnection(parentConn);
        }
    }

    public void terminate() {
        // 1. unregister node from coordinator
        try {
            unregisterNode();
        } catch (IOException e) {
            logger.warn("Failed to unregister a node", e);
        }
        // 2. terminate server socket (no more accept child connection)
        terminate.set(true);
    }

    private void unregisterNode() throws IOException {
        final String sessionName = context.getSessionName();
        final Socket sock = connectToCoordinator();
        final DataOutputStream out = NetUtils.getDataOutputStream(sock);
        try {
            // 1. write a command
            IOUtils.writeEnum(Command.unregisterNode, out);
            // 2. write a session name
            IOUtils.writeString(sessionName, out);
            // 3. write the local node
            NodeInfo.writeTo(localNode, out);
            out.flush();
        } finally {
            IOUtils.closeQuietly(sock);
        }
    }

    private Socket connectToCoordinator() throws IOException {
        SocketAddress coordEndpoint = context.getCoordinatorEndpoint();
        return NetUtils.openSocket(coordEndpoint);
    }
}
