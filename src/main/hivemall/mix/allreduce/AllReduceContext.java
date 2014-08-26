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
import hivemall.utils.io.IOUtils;
import hivemall.utils.net.NetUtils;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

public final class AllReduceContext implements Closeable {

    private final SocketAddress coordEndpoint;
    private final String sessionName;

    private ConnectionInfo parentConnection;
    private List<ConnectionInfo> childConnections;

    public AllReduceContext(String coordURI, String sessionName) {
        if(coordURI == null) {
            throw new IllegalArgumentException("coordURI was NULL");
        }
        if(sessionName == null) {
            throw new IllegalArgumentException("sessionName was NULL");
        }
        this.coordEndpoint = NetUtils.getSocketAddress(coordURI);
        this.sessionName = sessionName;
        this.parentConnection = null;
        this.childConnections = new ArrayList<ConnectionInfo>();
    }

    public SocketAddress getCoordinatorEndpoint() {
        return coordEndpoint;
    }

    public String getSessionName() {
        return sessionName;
    }

    public void setParentConnection(ConnectionInfo conn) {
        this.parentConnection = conn;
    }

    public void addChildConnection(ConnectionInfo conn) {
        childConnections.add(conn);
    }

    public boolean isRoot() {
        return parentConnection != null;
    }

    public DataInputStream getParentDataInputStream() {
        return parentConnection.getDataInputStream();
    }

    public DataOutputStream getParentDataOutputStream() {
        return parentConnection.getDataOutputStream();
    }

    public Iterable<DataInputStream> getChildrenDataInputStreams() {
        final List<DataInputStream> iterable = new ArrayList<DataInputStream>(childConnections.size());
        for(ConnectionInfo childInfo : childConnections) {
            iterable.add(childInfo.getDataInputStream());
        }
        return iterable;
    }

    public Iterable<DataOutputStream> getChildrenDataOutputStreams() {
        final List<DataOutputStream> iterable = new ArrayList<DataOutputStream>(childConnections.size());
        for(ConnectionInfo childInfo : childConnections) {
            iterable.add(childInfo.getDataOutputStream());
        }
        return iterable;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(parentConnection);
        for(ConnectionInfo childInfo : childConnections) {
            IOUtils.closeQuietly(childInfo);
        }
    }

}
