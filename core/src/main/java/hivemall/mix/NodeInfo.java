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
package hivemall.mix;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

public final class NodeInfo {

    private final InetAddress addr;
    private final int port;

    public NodeInfo(InetAddress addr, int port) {
        if (addr == null) {
            throw new IllegalArgumentException("addr is null");
        }
        this.addr = addr;
        this.port = port;
    }

    public NodeInfo(InetSocketAddress sockAddr) {
        this.addr = sockAddr.getAddress();
        this.port = sockAddr.getPort();
    }

    public InetAddress getAddress() {
        return addr;
    }

    public int getPort() {
        return port;
    }

    public SocketAddress getSocketAddress() {
        return new InetSocketAddress(addr, port);
    }

    @Override
    public int hashCode() {
        return addr.hashCode() + port;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof NodeInfo) {
            NodeInfo other = (NodeInfo) obj;
            return addr.equals(other.addr) && (port == other.port);
        }
        return false;
    }

    @Override
    public String toString() {
        return addr.toString() + ":" + port;
    }

}
